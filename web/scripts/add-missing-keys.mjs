import fs from 'node:fs/promises'
import path from 'node:path'

const LOCALES_DIR = path.resolve('src/i18n/locales')

function stableStringify(obj) {
  return JSON.stringify(obj, null, 2) + '\n'
}

const newKeys = {
  en: {
    'Async task polling concurrency': 'Async task polling concurrency',
    'Number of tasks polled concurrently per channel. Only applies to video/image async tasks (polled one request per task); Suno/Midjourney and non-task channels are unaffected. Set to 1 for serial polling. Default 10.':
      'Number of tasks polled concurrently per channel. Only applies to video/image async tasks (polled one request per task); Suno/Midjourney and non-task channels are unaffected. Set to 1 for serial polling. Default 10.',
  },
  zh: {
    'Async task polling concurrency': '异步任务轮询并发数',
    'Number of tasks polled concurrently per channel. Only applies to video/image async tasks (polled one request per task); Suno/Midjourney and non-task channels are unaffected. Set to 1 for serial polling. Default 10.':
      '每个渠道并发轮询的任务数。仅对视频/图片类异步任务生效（逐任务单独请求上游）；Suno/Midjourney 及非任务渠道不受影响。设为 1 表示串行轮询。默认 10。',
  },
  'zh-TW': {
    'Async task polling concurrency': '非同步任務輪詢並發數',
    'Number of tasks polled concurrently per channel. Only applies to video/image async tasks (polled one request per task); Suno/Midjourney and non-task channels are unaffected. Set to 1 for serial polling. Default 10.':
      '每個渠道並發輪詢的任務數。僅對影片/圖片類非同步任務生效（逐任務單獨請求上游）；Suno/Midjourney 及非任務渠道不受影響。設為 1 表示序列輪詢。預設 10。',
  },
  fr: {
    'Async task polling concurrency':
      "Concurrence d'interrogation des tâches asynchrones",
    'Number of tasks polled concurrently per channel. Only applies to video/image async tasks (polled one request per task); Suno/Midjourney and non-task channels are unaffected. Set to 1 for serial polling. Default 10.':
      "Nombre de tâches interrogées simultanément par canal. S'applique uniquement aux tâches asynchrones vidéo/image (une requête par tâche) ; Suno/Midjourney et les canaux sans tâche ne sont pas affectés. Définir sur 1 pour une interrogation en série. Par défaut 10.",
  },
  ja: {
    'Async task polling concurrency': '非同期タスクポーリングの並列数',
    'Number of tasks polled concurrently per channel. Only applies to video/image async tasks (polled one request per task); Suno/Midjourney and non-task channels are unaffected. Set to 1 for serial polling. Default 10.':
      'チャネルごとに同時にポーリングするタスク数。動画・画像の非同期タスクにのみ適用されます（タスクごとに個別リクエスト）。Suno/Midjourney やタスク以外のチャネルには影響しません。1 に設定すると順次ポーリングになります。デフォルトは 10。',
  },
  ru: {
    'Async task polling concurrency':
      'Параллельность опроса асинхронных задач',
    'Number of tasks polled concurrently per channel. Only applies to video/image async tasks (polled one request per task); Suno/Midjourney and non-task channels are unaffected. Set to 1 for serial polling. Default 10.':
      'Количество задач, опрашиваемых одновременно для одного канала. Применяется только к асинхронным задачам видео/изображений (по одному запросу на задачу); Suno/Midjourney и каналы без задач не затрагиваются. Установите 1 для последовательного опроса. По умолчанию 10.',
  },
  vi: {
    'Async task polling concurrency':
      'Số luồng thăm dò tác vụ bất đồng bộ',
    'Number of tasks polled concurrently per channel. Only applies to video/image async tasks (polled one request per task); Suno/Midjourney and non-task channels are unaffected. Set to 1 for serial polling. Default 10.':
      'Số tác vụ được thăm dò đồng thời cho mỗi kênh. Chỉ áp dụng cho tác vụ bất đồng bộ video/hình ảnh (mỗi tác vụ một yêu cầu); Suno/Midjourney và các kênh không phải tác vụ không bị ảnh hưởng. Đặt là 1 để thăm dò tuần tự. Mặc định 10.',
  },
}

async function main() {
  let totalAdded = 0

  for (const [locale, trans] of Object.entries(newKeys)) {
    const filePath = path.join(LOCALES_DIR, `${locale}.json`)
    const json = JSON.parse(await fs.readFile(filePath, 'utf8'))

    let count = 0
    for (const [key, value] of Object.entries(trans)) {
      if (!Object.prototype.hasOwnProperty.call(json.translation, key)) {
        json.translation[key] = value
        count++
      } else if (json.translation[key] !== value) {
        json.translation[key] = value
        count++
      }
    }

    if (count > 0) {
      json.translation = Object.fromEntries(
        Object.entries(json.translation).sort(([a], [b]) => a.localeCompare(b))
      )
      await fs.writeFile(filePath, stableStringify(json), 'utf8')
    }

    console.log(`${locale}: ${count} translations applied`)
    totalAdded += count
  }

  console.log(`\nTotal: ${totalAdded} translations applied`)
}

main().catch((err) => {
  console.error(err)
  process.exitCode = 1
})
