require('dotenv').config()
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js')
const qrcode = require('qrcode-terminal')
const { createClient } = require('@supabase/supabase-js')
const ws = require('ws')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { execFile } = require('child_process')
const { promisify } = require('util')
const sharp = require('sharp')
const Tesseract = require('tesseract.js')
const cron = require('node-cron')

const execFileAsync = promisify(execFile)

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_KEY) {
  console.error('❌ SUPABASE_URL atau SUPABASE_SERVICE_KEY belum diisi di file .env')
  process.exit(1)
}

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  {
    auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
    realtime: { transport: ws },
  }
)

const TZ = 'Asia/Jakarta'

// Tanggal hari ini dalam zona WIB (YYYY-MM-DD), tidak bergantung TZ mesin.
function todayWIB() {
  return new Date(Date.now() + 7 * 60 * 60 * 1000).toISOString().slice(0, 10)
}

// Jam:menit sekarang dalam WIB (buat pencocokan jadwal cron).
function nowHHMM() {
  const now = new Date(new Date().toLocaleString('en-US', { timeZone: TZ }))
  return `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`
}

function fmtJam(iso) {
  if (!iso) return ''
  return new Date(iso)
    .toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', timeZone: TZ })
    .replace('.', ':')
}

// ── Absensi: .cekabsen (staff/murid/muridtg) ─────────────────────────
// Selaras dengan ichikara-wa-bot. Sumber data: tabel users + attendance.

const STAFF_ROLES = ['staff', 'sensei', 'asisten_sensei', 'employee', 'hrd', 'advisor']

async function handleCekAbsen(args, reply) {
  if (!args.length) {
    return reply('❌ Format salah. Gunakan: `.cekabsen staff`, `.cekabsen murid`, atau `.cekabsen muridtg`')
  }

  const roleType = args[0].toLowerCase()
  if (!['staff', 'murid', 'muridtg'].includes(roleType)) {
    return reply('❌ Tipe tidak dikenali. Gunakan: `.cekabsen staff`, `.cekabsen murid`, atau `.cekabsen muridtg`')
  }

  const { data: users, error: usersErr } = await supabase
    .from('users')
    .select('id, name, role')
    .eq('is_active', true)

  if (usersErr) {
    console.error('Error fetching users:', usersErr.message)
    return reply('❌ Gagal mengambil data user dari sistem absensi.')
  }

  const targetUsers = users.filter(u => {
    if (roleType === 'staff') return STAFF_ROLES.includes(u.role)
    if (roleType === 'muridtg') return String(u.role).toLowerCase() === 'murid_tg'
    return !STAFF_ROLES.includes(u.role) && String(u.role).toLowerCase() !== 'murid_tg'
  })

  if (!targetUsers.length) {
    return reply(`Tidak ada data aktif ditemukan untuk kategori: ${roleType}.`)
  }

  const todayStr = todayWIB()
  const userIds = targetUsers.map(u => u.id)

  const { data: attendance, error: attErr } = await supabase
    .from('attendance')
    .select('user_id, check_in_at, absence_reason')
    .eq('date', todayStr)
    .in('user_id', userIds)

  if (attErr) {
    console.error('Error fetching attendance:', attErr.message)
    return reply('❌ Gagal mengambil data kehadiran harian.')
  }

  const attnMap = {}
  for (const a of attendance || []) attnMap[a.user_id] = a

  const hadirList = []
  const sakitList = []
  const izinList = []
  const belumHadirList = []
  let countHadir = 0

  for (const u of targetUsers) {
    const att = attnMap[u.id]
    if (att && att.check_in_at) {
      hadirList.push(`- ${u.name} (${fmtJam(att.check_in_at)})`)
      countHadir++
    } else if (att && att.absence_reason) {
      const reason = att.absence_reason.toLowerCase()
      if (reason === 'sakit') sakitList.push(`- ${u.name}`)
      else if (reason === 'izin') izinList.push(`- ${u.name}`)
      else belumHadirList.push(`- ${u.name} (*${att.absence_reason}*)`)
    } else {
      belumHadirList.push(`- ${u.name}`)
    }
  }

  let msg = `*Rekap Absensi ${roleType.toUpperCase()} - ${todayStr}*\n`
  msg += `Tingkat Kehadiran: ${countHadir}/${targetUsers.length}\n\n`
  msg += `*Hadir:*\n${hadirList.length ? hadirList.join('\n') : '-'}\n\n`
  msg += `*Belum Hadir:*\n${belumHadirList.length ? belumHadirList.join('\n') : '-'}\n\n`
  msg += `*Sakit:*\n${sakitList.length ? sakitList.join('\n') : '-'}\n\n`
  msg += `*Izin:*\n${izinList.length ? izinList.join('\n') : '-'}\n`
  msg += `\n_Data ditarik otomatis dari portal absensi (Read-Only)._`

  return reply(msg.trim())
}

// ── Absensi event: .cekevent <keyword> ───────────────────────────────
// Sumber data: events + event_participants + event_attendance.

async function generateEventReport(event, dateStr) {
  const { data: participants, error: pErr } = await supabase
    .from('event_participants')
    .select('id, users(id, name, role)')
    .eq('event_id', event.id)

  if (pErr || !participants || !participants.length) {
    return `*Rekap Absensi Event: ${event.name}*\nTanggal: ${dateStr}\n\nBelum ada peserta terdaftar di event ini.`
  }

  const pIds = participants.map(p => p.id)
  const { data: attendance, error: attErr } = await supabase
    .from('event_attendance')
    .select('event_participant_id, check_in_at, status, notes')
    .eq('date', dateStr)
    .in('event_participant_id', pIds)

  if (attErr) throw new Error('Gagal mengambil data kehadiran harian event.')

  const attnMap = {}
  for (const a of attendance || []) attnMap[a.event_participant_id] = a

  const hadirList = []
  const izinList = []
  const belumHadirList = []
  let countHadir = 0

  for (const p of participants) {
    const uName = p.users ? p.users.name : 'Unknown'
    const att = attnMap[p.id]
    if (att) {
      const isHadir = att.status === 'hadir' || !att.status
      if (isHadir) {
        const t = fmtJam(att.check_in_at)
        hadirList.push(`- ${uName}${t ? ` (${t})` : ''}`)
        countHadir++
      } else {
        const notes = att.notes ? `: ${att.notes}` : ''
        izinList.push(`- ${uName} (${att.status.toUpperCase()}${notes})`)
      }
    } else {
      belumHadirList.push(`- ${uName}`)
    }
  }

  let msg = `*Rekap Absensi Event: ${event.name}*\nTanggal: ${dateStr}\n`
  msg += `Tingkat Kehadiran: ${countHadir}/${participants.length}\n\n`
  if (hadirList.length) msg += `*Hadir:*\n${hadirList.join('\n')}\n\n`
  if (izinList.length) msg += `*Keterangan (Izin/Sakit):*\n${izinList.join('\n')}\n\n`
  if (belumHadirList.length) msg += `*Belum Hadir:*\n${belumHadirList.join('\n')}\n`
  msg += `\n_Data ditarik otomatis dari portal absensi event_`
  return msg.trim()
}

async function handleCekEvent(args, reply) {
  if (!args.length) {
    return reply('❌ Format salah. Gunakan: `.cekevent <nama_event/kata_kunci>`\nContoh: `.cekevent jishujikan`')
  }
  const keyword = args.join(' ')
  try {
    const { data: events, error } = await supabase
      .from('events')
      .select('*')
      .ilike('name', `%${keyword}%`)
      .order('created_at', { ascending: false })
      .limit(1)

    if (error || !events || !events.length) {
      return reply(`❌ Event dengan kata kunci "${keyword}" tidak ditemukan.`)
    }
    const report = await generateEventReport(events[0], todayWIB())
    return reply(report)
  } catch (err) {
    console.error('Error cekevent:', err.message)
    return reply('❌ Terjadi kesalahan saat menarik data event.')
  }
}

// ── Jadwal rekap otomatis: .sched ────────────────────────────────────

async function handleSched(args, reply, msg) {
  if (!args.length) {
    return reply('❌ Format salah.\nKetik: `.sched absen 17:00 staff` atau `.sched event 19:30 jishujikan`\n`.sched list` / `.sched del <id>`')
  }

  const sub = args[0].toLowerCase()

  if (sub === 'list') {
    const { data, error } = await supabase.from('recap_schedules').select('*')
    if (error) {
      if (error.code === '42P01') return reply('❌ Tabel recap_schedules belum dibuat di database.')
      return reply(`❌ Gagal mengambil daftar jadwal: ${error.message}`)
    }
    if (!data || !data.length) return reply('Kosong. Belum ada jadwal terdaftar.')
    let out = '*Daftar Jadwal Rekap Otomatis:*\n'
    data.forEach((row, i) => {
      const target = row.nama_grup || row.target_jid
      out += `\n${i + 1}. [${row.id.split('-')[0]}] ${row.jenis_recap.toUpperCase()} - ${row.waktu} WIB\n👉 Param: ${row.parameter}\n👉 Target: ${target}\n`
    })
    return reply(out)
  }

  if (sub === 'del') {
    const idFragment = args[1]
    if (!idFragment) return reply('❌ Masukkan ID jadwal yang ingin dihapus. (Contoh: .sched del 1234abcd)')
    const { data, error } = await supabase.from('recap_schedules').select('id')
    if (error) return reply(`❌ Error DB: ${error.message}`)
    const target = data.find(r => r.id.startsWith(idFragment))
    if (!target) return reply(`❌ Jadwal dengan awalan ID "${idFragment}" tidak ditemukan.`)
    const delRes = await supabase.from('recap_schedules').delete().eq('id', target.id)
    if (delRes.error) return reply(`❌ Gagal menghapus jadwal: ${delRes.error.message}`)
    return reply('✅ Jadwal berhasil dihapus.')
  }

  if (sub === 'absen' || sub === 'event') {
    const waktu = args[1]
    const parameter = args.slice(2).join(' ')
    if (!waktu || !parameter) {
      return reply(`❌ Format salah.\nContoh: \`.sched ${sub} ${sub === 'absen' ? '17:00 staff' : '19:30 jishujikan'}\``)
    }
    if (!/^\d{2}:\d{2}$/.test(waktu)) {
      return reply('❌ Format waktu harus HH:MM WIB (contoh: 17:00 atau 09:30)')
    }

    let nama_grup = 'Chat Pribadi'
    try {
      const chat = await msg.getChat()
      if (chat.isGroup) nama_grup = chat.name
    } catch (e) { }

    const { error } = await supabase.from('recap_schedules').insert({
      target_jid: msg.from,
      nama_grup,
      jenis_recap: sub,
      parameter,
      waktu,
    })
    if (error) {
      if (error.code === '42P01') return reply('❌ Tabel recap_schedules belum dibuat di database.')
      return reply(`❌ Gagal menyimpan jadwal: ${error.message}`)
    }
    return reply(`✅ Sukses mendaftarkan jadwal otomatis:\n- Tipe: ${sub.toUpperCase()}\n- Parameter: ${parameter}\n- Waktu: ${waktu} WIB\n- Dikirim ke: ${nama_grup}`)
  }

  return reply('❌ Sub-perintah tidak dikenali.')
}

// Cron per menit: cek jadwal yang waktunya cocok, lalu kirim ke target.
async function automatedRecapScheduler() {
  try {
    const currentWaktu = nowHHMM()
    const { data, error } = await supabase
      .from('recap_schedules')
      .select('*')
      .eq('status', 'active')
      .eq('waktu', currentWaktu)

    if (error) {
      if (error.code !== '42P01') console.error('[CRON] scheduler error:', error.message)
      return
    }
    if (!data.length) return

    console.log(`[CRON] Menjalankan ${data.length} jadwal untuk ${currentWaktu}`)
    for (const row of data) {
      const sendToTarget = (text) => client.sendMessage(row.target_jid, text)
      try {
        if (row.jenis_recap === 'absen') await handleCekAbsen(row.parameter.split(/\s+/), sendToTarget)
        else if (row.jenis_recap === 'event') await handleCekEvent([row.parameter], sendToTarget)
      } catch (err) {
        console.error(`[CRON] Error jadwal ID ${row.id}:`, err.message)
      }
    }
  } catch (e) {
    console.error('[CRON] automatedRecapScheduler gagal:', e.message)
  }
}

// ── Fitur media: downloader, sticker, OCR ─────────────────────────────

const WA_MEDIA_LIMIT = 64 * 1024 * 1024 // batas aman kirim media via WhatsApp

async function resolveMediaTarget(msg) {
  if (msg.hasQuotedMsg) {
    const quoted = await msg.getQuotedMessage()
    if (quoted && quoted.hasMedia) return quoted
  }
  if (msg.hasMedia) return msg
  return null
}

async function ytDownload(url) {
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wabot-dl-'))
  try {
    await execFileAsync('yt-dlp', [
      '--no-playlist',
      '--no-warnings',
      '--no-mtime',
      '-f', 'b[filesize<64M]/bv*[height<=720]+ba/b/best',
      '--merge-output-format', 'mp4',
      '-o', path.join(workDir, '%(id)s.%(ext)s'),
      url,
    ], { timeout: 180000, maxBuffer: 32 * 1024 * 1024 })

    const files = fs.readdirSync(workDir)
      .map(f => path.join(workDir, f))
      .filter(f => fs.statSync(f).isFile())
    return { files, workDir }
  } catch (err) {
    fs.rmSync(workDir, { recursive: true, force: true })
    throw err
  }
}

async function handleDownload(msg, url) {
  if (!url || !/^https?:\/\//i.test(url)) {
    await msg.reply('⚠️ Kasih link-nya. Contoh: `.yt https://youtu.be/xxxx`')
    return
  }
  await msg.reply('⏳ Lagi diproses, tunggu sebentar...')

  let workDir
  try {
    const res = await ytDownload(url)
    workDir = res.workDir
    if (!res.files.length) {
      await msg.reply('❌ Nggak ada media yang bisa diunduh dari link itu.')
      return
    }
    for (const file of res.files) {
      const size = fs.statSync(file).size
      if (size > WA_MEDIA_LIMIT) {
        await msg.reply(`⚠️ File "${path.basename(file)}" kegedean (${(size / 1048576).toFixed(1)}MB), dilewati.`)
        continue
      }
      await msg.reply(MessageMedia.fromFilePath(file))
    }
  } catch (err) {
    console.error('Download error:', err.message)
    await msg.reply('❌ Gagal download. Kemungkinan link salah, konten private, atau platform lagi berubah.')
  } finally {
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true })
  }
}

async function handleSticker(msg) {
  const target = await resolveMediaTarget(msg)
  if (!target) {
    await msg.reply('⚠️ Balas sebuah *gambar* dengan perintah `.sticker`.')
    return
  }
  try {
    const media = await target.downloadMedia()
    if (!media || !media.mimetype.startsWith('image')) {
      await msg.reply('⚠️ Media yang dibalas bukan gambar.')
      return
    }
    const webp = await sharp(Buffer.from(media.data, 'base64'))
      .resize(512, 512, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
      .webp()
      .toBuffer()
    const sticker = new MessageMedia('image/webp', webp.toString('base64'))
    await msg.reply(sticker, undefined, {
      sendMediaAsSticker: true,
      stickerName: 'Ichikara',
      stickerAuthor: 'Bot Ichikara',
    })
  } catch (err) {
    console.error('Sticker error:', err.message)
    await msg.reply('❌ Gagal bikin stiker.')
  }
}

async function handleOcr(msg) {
  const target = await resolveMediaTarget(msg)
  if (!target) {
    await msg.reply('⚠️ Balas sebuah *gambar* dengan perintah `.ocr`.')
    return
  }
  try {
    const media = await target.downloadMedia()
    if (!media || !media.mimetype.startsWith('image')) {
      await msg.reply('⚠️ Media yang dibalas bukan gambar.')
      return
    }
    await msg.reply('🔍 Lagi baca teks di gambar...')
    const { data: { text } } = await Tesseract.recognize(Buffer.from(media.data, 'base64'), 'ind+eng')
    const clean = text.trim()
    await msg.reply(clean ? `*Hasil OCR:*\n\n${clean}` : 'ℹ️ Nggak ada teks yang kebaca di gambar itu.')
  } catch (err) {
    console.error('OCR error:', err.message)
    await msg.reply('❌ Gagal baca teks dari gambar.')
  }
}

function helpText() {
  return `*Bot Ichikara — Daftar Perintah*\n\n` +
    `*ABSENSI*\n` +
    `.cekabsen staff|murid|muridtg   Rekap kehadiran harian per kategori\n` +
    `.cekevent <keyword>             Rekap kehadiran event (mis. jishujikan)\n\n` +
    `*JADWAL OTOMATIS*\n` +
    `.sched event 19:30 jishujikan   Jadwalkan rekap event ke chat ini\n` +
    `.sched absen 17:00 staff        Jadwalkan rekap absen ke chat ini\n` +
    `.sched list                     Lihat semua jadwal aktif\n` +
    `.sched del <id>                 Hapus jadwal\n\n` +
    `*DOWNLOADER* (tempel link setelah perintah)\n` +
    `.yt <link>       Download video YouTube\n` +
    `.ig <link>       Download media Instagram\n` +
    `.fb <link>       Download video Facebook\n` +
    `.threads <link>  Download media Threads\n` +
    `.dl <link>       Auto-deteksi platform\n\n` +
    `*TOOLS*\n` +
    `.sticker    Balas sebuah gambar untuk jadi stiker (alias .s)\n` +
    `.ocr        Balas sebuah gambar untuk baca teksnya\n` +
    `.id         Tampilkan ID chat/grup ini\n` +
    `.help       Tampilkan daftar perintah ini`
}

const client = new Client({
  authStrategy: new LocalAuth(),
  puppeteer: { args: ['--no-sandbox', '--disable-setuid-sandbox'] },
})

client.on('qr', (qr) => {
  console.log('\n=== Scan QR ini dengan WhatsApp di HP Anda ===')
  qrcode.generate(qr, { small: true })
})

client.on('ready', () => {
  console.log('✅ WhatsApp Bot Ichikara siap! Kirim .help untuk daftar perintah.')
  cron.schedule('* * * * *', automatedRecapScheduler, { timezone: TZ })
  console.log('⏰ Scheduler rekap otomatis aktif (cek tiap menit, WIB).')
})

client.on('auth_failure', () => {
  console.error('❌ Autentikasi gagal. Hapus folder .wwebjs_auth lalu jalankan ulang.')
})

client.on('message', async (msg) => {
  const raw = (msg.body || '').trim()
  if (!raw.startsWith('.')) return

  const [cmdRaw, ...args] = raw.slice(1).split(/\s+/)
  const cmd = cmdRaw.toLowerCase()
  const reply = (text) => msg.reply(text)

  try {
    switch (cmd) {
      case 'cekabsen': await handleCekAbsen(args, reply); break
      case 'cekevent': await handleCekEvent(args, reply); break
      case 'sched': await handleSched(args, reply, msg); break
      case 'yt':
      case 'ig':
      case 'fb':
      case 'threads':
      case 'dl':
        await handleDownload(msg, args.join(' ')); break
      case 'sticker':
      case 's':
        await handleSticker(msg); break
      case 'ocr': await handleOcr(msg); break
      case 'id': await reply(`*ID chat ini:*\n\`\`\`${msg.from}\`\`\``); break
      case 'help': await reply(helpText()); break
    }
  } catch (err) {
    console.error(`Error saat menjalankan .${cmd}:`, err.message)
    await reply('❌ Terjadi kesalahan. Coba lagi nanti.')
  }
})

// Validasi koneksi Supabase sebelum start WhatsApp
async function checkSupabaseConnection() {
  const { error } = await supabase.from('users').select('id').limit(1)
  if (error) {
    console.error('❌ Gagal konek ke Supabase:', error.message)
    console.error('   Pastikan SUPABASE_SERVICE_KEY di file .env adalah "service_role" key (bukan anon key)')
    process.exit(1)
  }
  console.log('✅ Koneksi Supabase berhasil')
}

checkSupabaseConnection().then(() => client.initialize())
