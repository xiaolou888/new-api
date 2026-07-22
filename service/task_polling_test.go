package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/bytedance/gopkg/util/gopool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type taskPollingFetchAdaptor struct {
	mu           sync.Mutex
	taskIDs      []string
	fetched      chan string
	blockTaskID  string
	blockStarted chan struct{}
	releaseBlock chan struct{}
	blockOnce    sync.Once
}

type sunoFailurePollingAdaptor struct {
	failReason string
}

func (a *sunoFailurePollingAdaptor) Init(_ *relaycommon.RelayInfo) {}

func (a *sunoFailurePollingAdaptor) FetchTask(_ string, _ string, body map[string]any, _ string) (*http.Response, error) {
	taskIDs, _ := body["ids"].([]string)
	items := make([]dto.SunoDataResponse, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		items = append(items, dto.SunoDataResponse{
			TaskID:     taskID,
			Status:     string(model.TaskStatusFailure),
			FailReason: a.failReason,
			FinishTime: time.Now().Unix(),
		})
	}

	responseBody, err := common.Marshal(dto.TaskResponse[[]dto.SunoDataResponse]{
		Code: dto.TaskSuccessCode,
		Data: items,
	})
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(responseBody)),
	}, nil
}

func (a *sunoFailurePollingAdaptor) ParseTaskResult([]byte) (*relaycommon.TaskInfo, error) {
	return nil, nil
}

func (a *sunoFailurePollingAdaptor) AdjustBillingOnComplete(_ *model.Task, _ *relaycommon.TaskInfo) int {
	return 0
}

func (a *taskPollingFetchAdaptor) Init(_ *relaycommon.RelayInfo) {}

func (a *taskPollingFetchAdaptor) FetchTask(_ string, _ string, body map[string]any, _ string) (*http.Response, error) {
	taskID, _ := body["task_id"].(string)
	if taskID == a.blockTaskID && a.releaseBlock != nil {
		a.blockOnce.Do(func() {
			if a.blockStarted != nil {
				close(a.blockStarted)
			}
		})
		<-a.releaseBlock
	}

	a.mu.Lock()
	a.taskIDs = append(a.taskIDs, taskID)
	a.mu.Unlock()
	if a.fetched != nil {
		select {
		case a.fetched <- taskID:
		default:
		}
	}

	response := dto.TaskResponse[model.Task]{
		Code: dto.TaskSuccessCode,
		Data: model.Task{
			TaskID:   taskID,
			Status:   model.TaskStatusInProgress,
			Progress: "30%",
		},
	}
	responseBody, err := common.Marshal(response)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(responseBody)),
	}, nil
}

func (a *taskPollingFetchAdaptor) ParseTaskResult([]byte) (*relaycommon.TaskInfo, error) {
	return &relaycommon.TaskInfo{Status: model.TaskStatusInProgress}, nil
}

func (a *taskPollingFetchAdaptor) AdjustBillingOnComplete(_ *model.Task, _ *relaycommon.TaskInfo) int {
	return 0
}

func (a *taskPollingFetchAdaptor) fetchCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.taskIDs)
}

func (a *taskPollingFetchAdaptor) fetchedTaskIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.taskIDs...)
}

// seedTaskPollingChannel creates a Kling video channel for polling tests.
// concurrency<=0 leaves TaskPollingConcurrency unset (runtime falls back to the
// global default); concurrency==1 forces the serial polling path; concurrency>1
// enables the bounded per-channel concurrent polling path.
func seedTaskPollingChannel(t *testing.T, id int, disableSleep bool, concurrency int) {
	t.Helper()
	ch := &model.Channel{
		Id:     id,
		Type:   constant.ChannelTypeKling,
		Name:   "polling_channel",
		Key:    "sk-test",
		Status: common.ChannelStatusEnabled,
	}
	other := dto.ChannelOtherSettings{DisableTaskPollingSleep: disableSleep}
	if concurrency > 0 {
		c := concurrency
		other.TaskPollingConcurrency = &c
	}
	ch.SetOtherSettings(other)
	require.NoError(t, model.DB.Create(ch).Error)
}

func seedPollingTask(t *testing.T, channelID int, publicID string, upstreamID string) *model.Task {
	t.Helper()
	task := &model.Task{
		TaskID:    publicID,
		Platform:  constant.TaskPlatform("kling"),
		UserId:    1,
		ChannelId: channelID,
		Action:    constant.TaskActionGenerate,
		Status:    model.TaskStatusInProgress,
		Progress:  "30%",
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
		PrivateData: model.TaskPrivateData{
			UpstreamTaskID: upstreamID,
		},
	}
	require.NoError(t, model.DB.Create(task).Error)
	return task
}

func TestUpdateVideoTasksSerialSleepWaitsBetweenTasks(t *testing.T) {
	truncate(t)

	const channelID = 101
	// concurrency=1 forces the serial path, which keeps the 1s inter-task sleep.
	seedTaskPollingChannel(t, channelID, false, 1)
	first := seedPollingTask(t, channelID, "task_public_1", "upstream_1")
	second := seedPollingTask(t, channelID, "task_public_2", "upstream_2")

	adaptor := &taskPollingFetchAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := UpdateVideoTasks(ctx, constant.TaskPlatform("kling"), map[int][]string{
		channelID: {
			first.GetUpstreamTaskID(),
			second.GetUpstreamTaskID(),
		},
	}, map[string]*model.Task{
		first.GetUpstreamTaskID():  first,
		second.GetUpstreamTaskID(): second,
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, adaptor.fetchCount())
}

func TestUpdateVideoTasksCanSkipPollingSleepPerChannel(t *testing.T) {
	truncate(t)

	const channelID = 102
	// concurrency=1 keeps serial polling; disable_task_polling_sleep removes the
	// inter-task 1s sleep so both tasks are fetched within the window.
	seedTaskPollingChannel(t, channelID, true, 1)
	first := seedPollingTask(t, channelID, "task_public_3", "upstream_3")
	second := seedPollingTask(t, channelID, "task_public_4", "upstream_4")

	adaptor := &taskPollingFetchAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := UpdateVideoTasks(ctx, constant.TaskPlatform("kling"), map[int][]string{
		channelID: {
			first.GetUpstreamTaskID(),
			second.GetUpstreamTaskID(),
		},
	}, map[string]*model.Task{
		first.GetUpstreamTaskID():  first,
		second.GetUpstreamTaskID(): second,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, adaptor.fetchCount())
}

func TestUpdateVideoTasksSerialSleepDoesNotBlockOtherChannels(t *testing.T) {
	truncate(t)

	const firstChannelID = 201
	const secondChannelID = 202
	// Both channels serial (concurrency=1) with the default 1s inter-task sleep;
	// channels are still polled in parallel, so within the short window each
	// channel fetches only its first task.
	seedTaskPollingChannel(t, firstChannelID, false, 1)
	seedTaskPollingChannel(t, secondChannelID, false, 1)
	firstChannelFirst := seedPollingTask(t, firstChannelID, "task_public_5", "upstream_a_1")
	firstChannelSecond := seedPollingTask(t, firstChannelID, "task_public_6", "upstream_a_2")
	secondChannelFirst := seedPollingTask(t, secondChannelID, "task_public_7", "upstream_b_1")
	secondChannelSecond := seedPollingTask(t, secondChannelID, "task_public_8", "upstream_b_2")

	adaptor := &taskPollingFetchAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := UpdateVideoTasks(ctx, constant.TaskPlatform("kling"), map[int][]string{
		firstChannelID: {
			firstChannelFirst.GetUpstreamTaskID(),
			firstChannelSecond.GetUpstreamTaskID(),
		},
		secondChannelID: {
			secondChannelFirst.GetUpstreamTaskID(),
			secondChannelSecond.GetUpstreamTaskID(),
		},
	}, map[string]*model.Task{
		firstChannelFirst.GetUpstreamTaskID():   firstChannelFirst,
		firstChannelSecond.GetUpstreamTaskID():  firstChannelSecond,
		secondChannelFirst.GetUpstreamTaskID():  secondChannelFirst,
		secondChannelSecond.GetUpstreamTaskID(): secondChannelSecond,
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ElementsMatch(t, []string{"upstream_a_1", "upstream_b_1"}, adaptor.fetchedTaskIDs())
}

func TestUpdateVideoTasksSlowChannelDoesNotBlockOtherChannels(t *testing.T) {
	truncate(t)

	const slowChannelID = 251
	const fastChannelID = 252
	// Both channels serial (concurrency=1); the fast channel skips the sleep so
	// its two tasks are fetched in deterministic order while the slow channel's
	// single task blocks on the upstream call.
	seedTaskPollingChannel(t, slowChannelID, false, 1)
	seedTaskPollingChannel(t, fastChannelID, true, 1)
	slowTask := seedPollingTask(t, slowChannelID, "task_public_slow", "upstream_slow_1")
	fastFirst := seedPollingTask(t, fastChannelID, "task_public_fast_1", "upstream_fast_parallel_1")
	fastSecond := seedPollingTask(t, fastChannelID, "task_public_fast_2", "upstream_fast_parallel_2")

	adaptor := &taskPollingFetchAdaptor{
		fetched:      make(chan string, 4),
		blockTaskID:  slowTask.GetUpstreamTaskID(),
		blockStarted: make(chan struct{}),
		releaseBlock: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseBlockedTask := func() {
		releaseOnce.Do(func() {
			close(adaptor.releaseBlock)
		})
	}
	t.Cleanup(releaseBlockedTask)
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	errCh := make(chan error, 1)
	gopool.Go(func() {
		errCh <- UpdateVideoTasks(context.Background(), constant.TaskPlatform("kling"), map[int][]string{
			slowChannelID: {
				slowTask.GetUpstreamTaskID(),
			},
			fastChannelID: {
				fastFirst.GetUpstreamTaskID(),
				fastSecond.GetUpstreamTaskID(),
			},
		}, map[string]*model.Task{
			slowTask.GetUpstreamTaskID():   slowTask,
			fastFirst.GetUpstreamTaskID():  fastFirst,
			fastSecond.GetUpstreamTaskID(): fastSecond,
		})
	})

	select {
	case <-adaptor.blockStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("slow channel did not start blocking")
	}

	require.Eventually(t, func() bool {
		fetchedTaskIDs := adaptor.fetchedTaskIDs()
		return len(fetchedTaskIDs) == 2 &&
			fetchedTaskIDs[0] == fastFirst.GetUpstreamTaskID() &&
			fetchedTaskIDs[1] == fastSecond.GetUpstreamTaskID()
	}, 500*time.Millisecond, 10*time.Millisecond)

	releaseBlockedTask()
	require.NoError(t, <-errCh)
	assert.ElementsMatch(t, []string{
		slowTask.GetUpstreamTaskID(),
		fastFirst.GetUpstreamTaskID(),
		fastSecond.GetUpstreamTaskID(),
	}, adaptor.fetchedTaskIDs())
}

func TestUpdateVideoTasksMixedChannelSleepSettings(t *testing.T) {
	truncate(t)

	const sleepyChannelID = 301
	const fastChannelID = 302
	// Both channels serial (concurrency=1): the sleepy channel keeps the 1s
	// inter-task sleep (only its first task is fetched in the window) while the
	// fast channel skips it (both tasks fetched).
	seedTaskPollingChannel(t, sleepyChannelID, false, 1)
	seedTaskPollingChannel(t, fastChannelID, true, 1)
	sleepyFirst := seedPollingTask(t, sleepyChannelID, "task_public_9", "upstream_sleepy_1")
	sleepySecond := seedPollingTask(t, sleepyChannelID, "task_public_10", "upstream_sleepy_2")
	fastFirst := seedPollingTask(t, fastChannelID, "task_public_11", "upstream_fast_1")
	fastSecond := seedPollingTask(t, fastChannelID, "task_public_12", "upstream_fast_2")

	adaptor := &taskPollingFetchAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := UpdateVideoTasks(ctx, constant.TaskPlatform("kling"), map[int][]string{
		sleepyChannelID: {
			sleepyFirst.GetUpstreamTaskID(),
			sleepySecond.GetUpstreamTaskID(),
		},
		fastChannelID: {
			fastFirst.GetUpstreamTaskID(),
			fastSecond.GetUpstreamTaskID(),
		},
	}, map[string]*model.Task{
		sleepyFirst.GetUpstreamTaskID():  sleepyFirst,
		sleepySecond.GetUpstreamTaskID(): sleepySecond,
		fastFirst.GetUpstreamTaskID():    fastFirst,
		fastSecond.GetUpstreamTaskID():   fastSecond,
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ElementsMatch(t, []string{"upstream_sleepy_1", "upstream_fast_1", "upstream_fast_2"}, adaptor.fetchedTaskIDs())
}

// concurrencyTrackingAdaptor records the peak number of concurrent FetchTask
// calls and blocks each call until released, so tests can assert concurrency
// bounds deterministically.
type concurrencyTrackingAdaptor struct {
	mu       sync.Mutex
	inflight int
	maxSeen  int
	fetched  int
	started  chan struct{}
	release  chan struct{}
}

func (a *concurrencyTrackingAdaptor) Init(_ *relaycommon.RelayInfo) {}

func (a *concurrencyTrackingAdaptor) FetchTask(_ string, _ string, body map[string]any, _ string) (*http.Response, error) {
	a.mu.Lock()
	a.inflight++
	if a.inflight > a.maxSeen {
		a.maxSeen = a.inflight
	}
	a.mu.Unlock()

	if a.started != nil {
		a.started <- struct{}{}
	}
	if a.release != nil {
		<-a.release
	}

	a.mu.Lock()
	a.inflight--
	a.fetched++
	a.mu.Unlock()

	taskID, _ := body["task_id"].(string)
	responseBody, err := common.Marshal(dto.TaskResponse[model.Task]{
		Code: dto.TaskSuccessCode,
		Data: model.Task{TaskID: taskID, Status: model.TaskStatusInProgress, Progress: "30%"},
	})
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(responseBody)),
	}, nil
}

func (a *concurrencyTrackingAdaptor) ParseTaskResult([]byte) (*relaycommon.TaskInfo, error) {
	return &relaycommon.TaskInfo{Status: model.TaskStatusInProgress}, nil
}

func (a *concurrencyTrackingAdaptor) AdjustBillingOnComplete(_ *model.Task, _ *relaycommon.TaskInfo) int {
	return 0
}

func (a *concurrencyTrackingAdaptor) maxConcurrent() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.maxSeen
}

func (a *concurrencyTrackingAdaptor) fetchedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.fetched
}

func TestUpdateVideoTasksConcurrentPollingSkipsInterTaskSleep(t *testing.T) {
	truncate(t)

	const channelID = 501
	// concurrency=5 enables the concurrent path; even without disabling the
	// sleep, tasks are polled in parallel with no 1s inter-task pacing.
	seedTaskPollingChannel(t, channelID, false, 5)
	first := seedPollingTask(t, channelID, "task_public_c1", "upstream_c1")
	second := seedPollingTask(t, channelID, "task_public_c2", "upstream_c2")
	third := seedPollingTask(t, channelID, "task_public_c3", "upstream_c3")

	adaptor := &taskPollingFetchAdaptor{}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	// A window far shorter than the 3x1s the serial path would require; the
	// concurrent path must still fetch all three tasks.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := UpdateVideoTasks(ctx, constant.TaskPlatform("kling"), map[int][]string{
		channelID: {
			first.GetUpstreamTaskID(),
			second.GetUpstreamTaskID(),
			third.GetUpstreamTaskID(),
		},
	}, map[string]*model.Task{
		first.GetUpstreamTaskID():  first,
		second.GetUpstreamTaskID(): second,
		third.GetUpstreamTaskID():  third,
	})

	require.NoError(t, err)
	assert.Equal(t, 3, adaptor.fetchCount())
}

func TestUpdateVideoTasksGlobalConcurrencyCapLimitsInFlightFetches(t *testing.T) {
	truncate(t)

	const channelID = 601
	// High per-channel concurrency, but the global cap must bound total in-flight fetches.
	seedTaskPollingChannel(t, channelID, true, 10)
	const taskCount = 6
	tasks := make(map[string]*model.Task, taskCount)
	upstreamIDs := make([]string, 0, taskCount)
	for i := 0; i < taskCount; i++ {
		upstreamID := fmt.Sprintf("upstream_g%d", i)
		task := seedPollingTask(t, channelID, fmt.Sprintf("task_public_g%d", i), upstreamID)
		tasks[upstreamID] = task
		upstreamIDs = append(upstreamIDs, upstreamID)
	}

	previousGlobal := constant.TaskPollGlobalConcurrency
	constant.TaskPollGlobalConcurrency = 2
	t.Cleanup(func() { constant.TaskPollGlobalConcurrency = previousGlobal })

	adaptor := &concurrencyTrackingAdaptor{
		started: make(chan struct{}, taskCount),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(adaptor.release) }) }
	t.Cleanup(releaseAll)
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	errCh := make(chan error, 1)
	gopool.Go(func() {
		errCh <- UpdateVideoTasks(context.Background(), constant.TaskPlatform("kling"), map[int][]string{
			channelID: upstreamIDs,
		}, tasks)
	})

	// Exactly the cap should be able to start; wait for those two.
	for i := 0; i < 2; i++ {
		select {
		case <-adaptor.started:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected two concurrent fetches to start")
		}
	}
	// No third fetch may start while the first two are still in flight.
	select {
	case <-adaptor.started:
		t.Fatal("global concurrency cap exceeded: a third fetch started")
	case <-time.After(100 * time.Millisecond):
	}

	releaseAll()
	require.NoError(t, <-errCh)
	assert.Equal(t, taskCount, adaptor.fetchedCount())
	assert.LessOrEqual(t, adaptor.maxConcurrent(), 2)
}

func TestUpdateVideoTasksFetchTimeoutAbandonsWaitAndKeepsTaskPending(t *testing.T) {
	truncate(t)

	const channelID = 701
	seedTaskPollingChannel(t, channelID, true, 2)
	blocked := seedPollingTask(t, channelID, "task_public_timeout", "upstream_timeout")

	previousTimeout := constant.TaskPollFetchTimeoutSeconds
	constant.TaskPollFetchTimeoutSeconds = 1
	t.Cleanup(func() { constant.TaskPollFetchTimeoutSeconds = previousTimeout })

	adaptor := &taskPollingFetchAdaptor{
		blockTaskID:  blocked.GetUpstreamTaskID(),
		blockStarted: make(chan struct{}),
		releaseBlock: make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(adaptor.releaseBlock) }) }
	t.Cleanup(release)
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	errCh := make(chan error, 1)
	gopool.Go(func() {
		errCh <- UpdateVideoTasks(context.Background(), constant.TaskPlatform("kling"), map[int][]string{
			channelID: {blocked.GetUpstreamTaskID()},
		}, map[string]*model.Task{blocked.GetUpstreamTaskID(): blocked})
	})

	// Without the fetch timeout the blocked upstream call would hang forever;
	// the timeout must let the polling pass return on its own.
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("UpdateVideoTasks did not return after the fetch timeout")
	}

	// The abandoned task must remain unfinished so the next pass retries it.
	var reloaded model.Task
	require.NoError(t, model.DB.First(&reloaded, blocked.ID).Error)
	assert.EqualValues(t, model.TaskStatusInProgress, reloaded.Status)
}

func TestUpdateSunoTasksStalePollsRefundExactlyOnce(t *testing.T) {
	truncate(t)

	const userID, tokenID, channelID = 401, 401, 401
	const initialUserQuota, initialTokenQuota, taskQuota = 10_000, 6_000, 2_500
	const publicTaskID, upstreamTaskID = "suno_public_refund_once", "suno_upstream_refund_once"

	seedUser(t, userID, initialUserQuota)
	seedToken(t, tokenID, userID, "sk-suno-refund-once", initialTokenQuota)
	baseURL := "https://suno.invalid"
	require.NoError(t, model.DB.Create(&model.Channel{
		Id:      channelID,
		Type:    constant.ChannelTypeSunoAPI,
		Name:    "suno_refund_once",
		Key:     "sk-suno-channel",
		Status:  common.ChannelStatusEnabled,
		BaseURL: &baseURL,
	}).Error)

	task := makeTask(userID, channelID, taskQuota, tokenID, BillingSourceWallet, 0)
	task.TaskID = publicTaskID
	task.Platform = constant.TaskPlatformSuno
	task.Status = model.TaskStatusInProgress
	task.Progress = "50%"
	task.SubmitTime = model.TaskRefundLegacyCutoff
	task.PrivateData.UpstreamTaskID = upstreamTaskID
	require.NoError(t, model.DB.Create(task).Error)

	var firstPollTask model.Task
	var staleSecondPollTask model.Task
	require.NoError(t, model.DB.First(&firstPollTask, task.ID).Error)
	require.NoError(t, model.DB.First(&staleSecondPollTask, task.ID).Error)

	adaptor := &sunoFailurePollingAdaptor{failReason: "upstream failed"}
	previousFactory := GetTaskAdaptorFunc
	GetTaskAdaptorFunc = func(constant.TaskPlatform) TaskPollingAdaptor { return adaptor }
	t.Cleanup(func() { GetTaskAdaptorFunc = previousFactory })

	require.NoError(t, updateSunoTasks(context.Background(), channelID, []string{upstreamTaskID}, map[string]*model.Task{
		upstreamTaskID: &firstPollTask,
	}))
	require.NoError(t, updateSunoTasks(context.Background(), channelID, []string{upstreamTaskID}, map[string]*model.Task{
		upstreamTaskID: &staleSecondPollTask,
	}))

	var reloaded model.Task
	require.NoError(t, model.DB.First(&reloaded, task.ID).Error)
	assert.EqualValues(t, model.TaskStatusFailure, reloaded.Status)
	assert.Zero(t, reloaded.Quota)
	assert.Equal(t, initialUserQuota+taskQuota, getUserQuota(t, userID))
	assert.Equal(t, initialTokenQuota+taskQuota, getTokenRemainQuota(t, tokenID))
	assert.Equal(t, int64(1), countLogs(t))
}

func TestSweepUnrefundedFailedTasksRefundsModernTaskAndSkipsLegacy(t *testing.T) {
	truncate(t)

	const userID = 402
	const initialQuota, modernTaskQuota, legacyTaskQuota = 10_000, 1_200, 1_800
	seedUser(t, userID, initialQuota)

	modernTask := makeTask(userID, 0, modernTaskQuota, 0, BillingSourceWallet, 0)
	modernTask.TaskID = "modern_failed_pending_refund"
	modernTask.Status = model.TaskStatusFailure
	modernTask.Progress = "100%"
	modernTask.SubmitTime = model.TaskRefundLegacyCutoff
	modernTask.UpdatedAt = time.Now().Add(-time.Minute).Unix()
	require.NoError(t, model.DB.Create(modernTask).Error)

	legacyTask := makeTask(userID, 0, legacyTaskQuota, 0, BillingSourceWallet, 0)
	legacyTask.TaskID = "legacy_failed_without_refund"
	legacyTask.Status = model.TaskStatusFailure
	legacyTask.Progress = "100%"
	legacyTask.SubmitTime = model.TaskRefundLegacyCutoff - 1
	legacyTask.UpdatedAt = time.Now().Add(-time.Minute).Unix()
	require.NoError(t, model.DB.Create(legacyTask).Error)

	sweepUnrefundedFailedTasks(context.Background())
	sweepUnrefundedFailedTasks(context.Background())

	var reloadedModern model.Task
	var reloadedLegacy model.Task
	require.NoError(t, model.DB.First(&reloadedModern, modernTask.ID).Error)
	require.NoError(t, model.DB.First(&reloadedLegacy, legacyTask.ID).Error)
	assert.Zero(t, reloadedModern.Quota)
	assert.Equal(t, legacyTaskQuota, reloadedLegacy.Quota)
	assert.Equal(t, initialQuota+modernTaskQuota, getUserQuota(t, userID))
	assert.Equal(t, int64(1), countLogs(t))
}

func TestSweepUnrefundedFailedTasksRestoresMarkerAfterFundingFailure(t *testing.T) {
	truncate(t)

	const userID, subscriptionID, taskQuota = 404, 404, 900
	const subscriptionUsed int64 = 5_000
	seedUser(t, userID, 0)

	task := makeTask(userID, 0, taskQuota, 0, BillingSourceSubscription, subscriptionID)
	task.TaskID = "subscription_failed_pending_refund"
	task.Status = model.TaskStatusFailure
	task.Progress = "100%"
	task.SubmitTime = model.TaskRefundLegacyCutoff
	task.UpdatedAt = time.Now().Add(-time.Minute).Unix()
	require.NoError(t, model.DB.Create(task).Error)

	sweepUnrefundedFailedTasks(context.Background())

	var afterFailedRefund model.Task
	require.NoError(t, model.DB.First(&afterFailedRefund, task.ID).Error)
	assert.Equal(t, taskQuota, afterFailedRefund.Quota)
	assert.Equal(t, int64(0), countLogs(t))

	seedSubscription(t, subscriptionID, userID, 10_000, subscriptionUsed)
	require.NoError(t, model.DB.Model(&model.Task{}).
		Where("id = ?", task.ID).
		UpdateColumn("updated_at", time.Now().Add(-time.Minute).Unix()).Error)

	sweepUnrefundedFailedTasks(context.Background())

	var afterSuccessfulRetry model.Task
	require.NoError(t, model.DB.First(&afterSuccessfulRetry, task.ID).Error)
	assert.Zero(t, afterSuccessfulRetry.Quota)
	assert.Equal(t, subscriptionUsed-int64(taskQuota), getSubscriptionUsed(t, subscriptionID))
	assert.Equal(t, int64(1), countLogs(t))
}

// TestPatchDataStatusRewritesTopLevelStatusAndPreservesFields 验证 issue #5715 修复：
// 顶层 status 被改写为终态，其余字段（含超出 float64 安全范围的大整数）保留原始字节。
func TestPatchDataStatusRewritesTopLevelStatusAndPreservesFields(t *testing.T) {
	original := json.RawMessage(`{"status":"IN_PROGRESS","id":"vid_123","seq":1234567890123456789}`)
	patched, changed := patchDataStatus(original, model.TaskStatusFailure)
	require.True(t, changed)

	var fields map[string]json.RawMessage
	require.NoError(t, common.Unmarshal(patched, &fields))
	assert.Equal(t, `"FAILURE"`, string(fields["status"]))
	assert.Equal(t, `"vid_123"`, string(fields["id"]))
	// 未经 interface{}/float64 解码，大整数原样保留。
	assert.Equal(t, `1234567890123456789`, string(fields["seq"]))
}

// TestPatchDataStatusLeavesDataUnchangedWhenNoTopLevelStatus 确认只有含顶层 status 键的
// JSON 对象才被改写；空值、非对象、无 status 键或非法 JSON 一律原样返回且 changed=false，
// 不破坏既有数据（含 ali 这类将状态放在嵌套路径的渠道）。
func TestPatchDataStatusLeavesDataUnchangedWhenNoTopLevelStatus(t *testing.T) {
	cases := []struct {
		name string
		data json.RawMessage
	}{
		{"empty", nil},
		{"no status key", json.RawMessage(`{"id":"vid_1"}`)},
		{"nested status only", json.RawMessage(`{"output":{"task_status":"RUNNING"}}`)},
		{"json array", json.RawMessage(`["status"]`)},
		{"json scalar", json.RawMessage(`"status"`)},
		{"invalid json", json.RawMessage(`{oops`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			patched, changed := patchDataStatus(tc.data, model.TaskStatusFailure)
			assert.False(t, changed)
			assert.Equal(t, string(tc.data), string(patched))
		})
	}
}

// TestSweepTimedOutTasksSyncsDataStatus 验证超时清理在写库前同步 Data 内嵌状态：
// 列状态与 data.status 均为 FAILURE，且 Data 其余字段保留（issue #5715）。
func TestSweepTimedOutTasksSyncsDataStatus(t *testing.T) {
	truncate(t)

	prev := constant.TaskTimeoutMinutes
	constant.TaskTimeoutMinutes = 15
	t.Cleanup(func() { constant.TaskTimeoutMinutes = prev })

	const userID, channelID = 501, 501
	seedUser(t, userID, 10_000)

	pastTs := time.Now().Add(-time.Hour).Unix()
	task := &model.Task{
		TaskID:     "timeout_data_sync",
		Platform:   constant.TaskPlatform("kling"),
		UserId:     userID,
		ChannelId:  channelID,
		Action:     constant.TaskActionGenerate,
		Status:     model.TaskStatusInProgress,
		Progress:   "30%",
		Quota:      0, // 无退款额度，隔离退款路径，仅验证 Data 同步。
		SubmitTime: pastTs,
		CreatedAt:  pastTs,
		UpdatedAt:  pastTs,
		Data:       json.RawMessage(`{"status":"IN_PROGRESS","id":"vid_1"}`),
	}
	require.NoError(t, model.DB.Create(task).Error)

	sweepTimedOutTasks(context.Background())

	var reloaded model.Task
	require.NoError(t, model.DB.First(&reloaded, task.ID).Error)
	assert.EqualValues(t, model.TaskStatusFailure, reloaded.Status)

	var fields map[string]json.RawMessage
	require.NoError(t, common.Unmarshal(reloaded.Data, &fields))
	assert.Equal(t, `"FAILURE"`, string(fields["status"]))
	assert.Equal(t, `"vid_1"`, string(fields["id"]))
}
