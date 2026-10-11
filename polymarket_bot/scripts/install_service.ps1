# ============================================================
# Установка polymarket_bot как службы Windows через NSSM.
#
# В отличие от run_supervised.bat, служба Windows:
#   - запускается автоматически при старте компьютера, ДО входа
#     в систему (не нужно логиниться, чтобы бот работал);
#   - сама перезапускается при падении (NSSM следит за процессом);
#   - переживает разрыв RDP/SSH-сессии, если вы управляете
#     компьютером удалённо - служба не привязана к сессии.
#
# Требуется NSSM (Non-Sucking Service Manager) - бесплатный,
# скачать: https://nssm.cc/download, распаковать nssm.exe куда
# угодно (например, в C:\nssm\nssm.exe) и указать путь ниже.
#
# Запускать от имени администратора (ПКМ по PowerShell -> "Запуск
# от имени администратора"), затем:
#   cd путь\к\проекту\scripts
#   .\install_service.ps1
# ============================================================

$ErrorActionPreference = "Stop"

# --- Настройте под себя ---
$NssmPath = "C:\nssm\nssm.exe"
$ServiceName = "PolymarketBot"
$ProjectRoot = (Resolve-Path "$PSScriptRoot\..").Path
$PythonExe = Join-Path $ProjectRoot "venv\Scripts\python.exe"
# --- конец настроек ---

if (-not (Test-Path $NssmPath)) {
    Write-Error "nssm.exe не найден по пути $NssmPath - скачайте с https://nssm.cc/download и поправьте `$NssmPath` в этом скрипте."
    exit 1
}
if (-not (Test-Path $PythonExe)) {
    Write-Error "Не найден venv\Scripts\python.exe - сначала создайте виртуальное окружение (python -m venv venv) и поставьте зависимости."
    exit 1
}

Write-Host "Устанавливаю службу '$ServiceName'..."
& $NssmPath install $ServiceName $PythonExe "-m uvicorn app.main:app --app-dir backend --host 127.0.0.1 --port 8000"
& $NssmPath set $ServiceName AppDirectory $ProjectRoot
& $NssmPath set $ServiceName AppStdout (Join-Path $ProjectRoot "data\service_stdout.log")
& $NssmPath set $ServiceName AppStderr (Join-Path $ProjectRoot "data\service_stderr.log")
& $NssmPath set $ServiceName AppRotateFiles 1
& $NssmPath set $ServiceName AppRotateBytes 10000000
# Перезапуск при падении: подождать 10с, пытаться всегда (0 = без лимита попыток).
& $NssmPath set $ServiceName AppExit Default Restart
& $NssmPath set $ServiceName AppRestartDelay 10000
& $NssmPath set $ServiceName Start SERVICE_AUTO_START
& $NssmPath set $ServiceName DisplayName "Polymarket NegRisk Bot"
& $NssmPath set $ServiceName Description "Сбор данных, сканер арбитража и (опционально) исполнение сделок на Polymarket. Управление через Telegram."

Write-Host ""
Write-Host "Готово. Служба '$ServiceName' установлена и настроена на автозапуск."
Write-Host "Запустить сейчас:    nssm start $ServiceName"
Write-Host "Остановить:          nssm stop $ServiceName"
Write-Host "Удалить службу:      nssm remove $ServiceName confirm"
Write-Host "Логи:                $ProjectRoot\data\service_stdout.log и service_stderr.log"
Write-Host "                     (плюс data\bot.log - собственный лог приложения с ротацией)"
