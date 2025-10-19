<#
.SYNOPSIS
使用宿主机pgbench客户端对Docker中的PostgreSQL和openGauss进行基准性能比对测试的PowerShell脚本。

.DESCRIPTION
该脚本通过宿主机的pgbench工具，分别对运行在宿主上的PostgreSQL和openGauss据库进行基准测试。
通过重启Docker容器实现，输出包括每次测试的详细日志、逐次结果CSV及聚合统计CSV。
使用前需确保pgbench已添加到系统PATH，可在PowerShell中直接调用。
#>

# ==============================================
# 配置参数区
# ==============================================

# 数据库主机地址与端口
$hostAddr = "127.0.0.1"
$postgresPort = 5432
$opengaussPort = 5433

# Docker容器名称（用于重启容器）
$postgresContainer = "postgresql" 
$opengaussContainer = "opengauss"

# 数据库登录凭据
$creds = @{
    "postgresql" = @{ user = "postgres"; pass = "123456"; port = $postgresPort }
    "opengauss"  = @{ user = "openGauss"; pass = "openGauss@123"; port = $opengaussPort }
}

# 基准测试参数配置
$SCALE = 50                  # pgbench初始化数据库的规模因子（数据量比例）
$clientsList = @(1,4,8,16,32,64)  # 测试的客户端数量列表 
$runs = 3                    # 每个客户端配置下的重复运行次数 
$DURATION = 120             # 每次测试的持续时间（秒） 
$threadRatio = 4             # 线程数与客户端数的比例（线程数=客户端数/比例，最小为1）
$DoColdCache = $true         # 是否启用冷缓存测试（$true启用，$false禁用）

# 输出目录与文件路径
$outdir = Join-Path -Path (Get-Location) -ChildPath ("pgbench_results_{0:yyyyMMdd_HHmmss}" -f (Get-Date))
New-Item -ItemType Directory -Path $outdir | Out-Null

$resultsCsv = Join-Path $outdir "results_runs.csv"
$summaryCsv = Join-Path $outdir "results_summary.csv"

# 初始化逐次结果CSV的表头
"DB,Clients,Threads,Run,Latency_Avg_ms,Init_Connection_Time_ms,TPS,LogFile,DockerStats_Before,DockerStats_After" | Out-File -FilePath $resultsCsv -Encoding utf8

# ==============================================
# 输出色彩格式
# ==============================================
$colorTitle = "Yellow"       # 模块标题色
$colorSuccess = "Green"      # 成功信息色
$colorWarning = "DarkYellow" # 警告信息色
$colorError = "Red"          # 错误信息色
$colorInfo = "Cyan"          # 普通信息色
$separator = "=" * 60        # 分隔线（用于模块区分）

# ==============================================
# 函数定义区
# ==============================================

<#
.SYNOPSIS
解析pgbench的输出文本，提取关键性能指标（TPS和延迟）。
#>
function Parse-PgbenchOutput {
    param ($text)
    $res = @{ Latency_Avg_ms = ""; Init_Connection_Time_ms = ""; TPS = "" }
    if ($null -eq $text) { return $res }

    $m1 = [regex]::Match($text, "latency average =\s*([\d\.]+)\s*ms")
    if ($m1.Success) { $res.Latency_Avg_ms = $m1.Groups[1].Value }

    $m2 = [regex]::Match($text, "initial connection time =\s*([\d\.]+)\s*ms")
    if ($m2.Success) { $res.Init_Connection_Time_ms = $m2.Groups[1].Value }

    $m3 = [regex]::Match($text, "tps =\s*([\d\.]+)\s*\(without")
    if ($m3.Success) { $res.TPS = $m3.Groups[1].Value }

    return $res
}

<#
.SYNOPSIS
获取指定Docker容器的实时统计信息（单次）。
#>
function Get-DockerStatsOnce {
    param ($containerName)
    $fmt = "Name: {{.Name}} CPUPerc: {{.CPUPerc}} MemUsage: {{.MemUsage}} NetIO: {{.NetIO}} BlockIO: {{.BlockIO}}"
    $stats = docker stats --no-stream --format $fmt $containerName 2>$null
    if ($LASTEXITCODE -ne 0) { return "获取容器状态失败" }
    return $stats.Trim()
}

<#
.SYNOPSIS
计算数组的中位数（用于统计TPS和延迟）。
#>
function Get-Median {
    param ($arr)
    if ($null -eq $arr -or $arr.Count -eq 0) { return $null }
    $sortedArr = $arr | Sort-Object
    $count = $sortedArr.Count
    if ($count % 2 -eq 1) {
        return $sortedArr[ [int]([math]::Floor($count/2)) ]
    } else {
        $midLeft = $sortedArr[$count/2 - 1]
        $midRight = $sortedArr[$count/2]
        return (($midLeft + $midRight) / 2.0)
    }
}

# ==============================================
# 1. 初始化数据库（创建pgbench测试表）
# ==============================================
Write-Host "`n$separator" -ForegroundColor $colorTitle
Write-Host "【1/3】初始化 pgbench 数据库（规模因子：$SCALE）" -ForegroundColor $colorTitle
Write-Host "$separator`n" -ForegroundColor $colorTitle

$dbList = @("postgresql", "opengauss")
$initCount = 1
foreach ($db in $dbList) {
    $dbUser = $creds[$db].user
    $dbPass = $creds[$db].pass
    $dbPort = $creds[$db].port

    Write-Host "`n$initCount/$($dbList.Count) 初始化目标：$db" -ForegroundColor $colorInfo
    Write-Host "参数配置 | 主机：$hostAddr | 端口：$dbPort | 用户名：$dbUser" -ForegroundColor $colorInfo

    $originalPwd = $env:PGPASSWORD
    $env:PGPASSWORD = $dbPass

    try {
        $initCmd = "pgbench -h $hostAddr -p $dbPort -U $dbUser -i -s $SCALE pgbench"
        Write-Host "执行命令：$initCmd`n" -ForegroundColor $colorInfo
        
        & pgbench -h $hostAddr -p $dbPort -U $dbUser -i -s $SCALE pgbench

        if ($LASTEXITCODE -eq 0) {
            Write-Host "`n✅ $db 初始化完成！" -ForegroundColor $colorSuccess
        } else {
            Write-Host "`n⚠️ $db 初始化异常！" -ForegroundColor $colorWarning
        }
    } catch {
        Write-Host "`n❌ $db 初始化失败！错误：$_" -ForegroundColor $colorError
    } finally {
        if ($null -ne $originalPwd) { $env:PGPASSWORD = $originalPwd } else { Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue }
    }
    $initCount++
}

# ==============================================
# 2. 执行基准测试
# ==============================================
Write-Host "`n`n$separator" -ForegroundColor $colorTitle
Write-Host "【2/3】执行基准测试（总任务：$($dbList.Count)库 × $($clientsList.Count)客户端 × $runs 次重复）" -ForegroundColor $colorTitle
Write-Host "$separator`n" -ForegroundColor $colorTitle


$allTestRecords = @()
$totalTestTasks = $dbList.Count * $clientsList.Count * $runs
$currentTask = 1

foreach ($db in $dbList) {
    $dbUser = $creds[$db].user
    $dbPass = $creds[$db].pass
    $dbPort = $creds[$db].port
    $containerName = if ($db -eq "postgresql") { $postgresContainer } else { $opengaussContainer }

    Write-Host "`n📌 当前测试数据库：$db（关联容器：$containerName）" -ForegroundColor $colorInfo

    foreach ($clientNum in $clientsList) {
        $threadNum = [math]::Max(1, [math]::Floor($clientNum / $threadRatio))

        for ($runIdx = 1; $runIdx -le $runs; $runIdx++) {
            Write-Host "`n$currentTask/$totalTestTasks 测试任务 | DB：$db | 客户端：$clientNum | 线程：$threadNum | 第 $runIdx 次" -ForegroundColor $colorInfo
            Write-Host "----------------------------------------------------------------------" -ForegroundColor $colorInfo

            $statsBeforeTest = ""
            if ($DoColdCache) {
                Write-Host "🔁 冷缓存模式：重启容器 $containerName（等待8秒确保服务启动）" -ForegroundColor $colorWarning
                docker restart $containerName | Out-Null
                Start-Sleep -Seconds 8
                $statsBeforeTest = Get-DockerStatsOnce -containerName $containerName
                Write-Host "✅ 容器重启完成 | 测试前状态：$statsBeforeTest" -ForegroundColor $colorSuccess
            } else {
                $statsBeforeTest = Get-DockerStatsOnce -containerName $containerName
                Write-Host "ℹ️ 热缓存模式 | 测试前状态：$statsBeforeTest" -ForegroundColor $colorInfoℹ
            }

            $testLogPath = Join-Path $outdir "$db`_client$clientNum`_run$runIdx.log"
            $testCmd = "pgbench -h $hostAddr -p $dbPort -U $dbUser -c $clientNum -j $threadNum -T $DURATION pgbench"
            
            Write-Host "`n🖊️ 执行测试命令：$testCmd" -ForegroundColor $colorInfo
            Write-Host "📂 测试日志将保存至：$testLogPath" -ForegroundColor $colorInfo

            $originalPwd = $env:PGPASSWORD
            $env:PGPASSWORD = $dbPass

            try {
                & pgbench -h $hostAddr -p $dbPort -U $dbUser -c $clientNum -j $threadNum -T $DURATION pgbench | Tee-Object -FilePath $testLogPath
                Write-Host "`n✅ 本次测试执行完成！" -ForegroundColor $colorSuccess
            } catch {
                Write-Host "`n❌ 本次测试执行失败！错误信息：$_" -ForegroundColor $colorError
            } finally {
                if ($null -ne $originalPwd) {
                    $env:PGPASSWORD = $originalPwd
                } else {
                    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
                }
            }

            $statsAfterTest = Get-DockerStatsOnce -containerName $containerName
            Write-Host "ℹ️ 测试后容器状态：$statsAfterTest" -ForegroundColor $colorInfo

            $testLogContent = Get-Content -Raw -LiteralPath $testLogPath -ErrorAction SilentlyContinue
            $parsedResult = Parse-PgbenchOutput -text $testLogContent
            Write-Host "📊 本次测试解析结果 | 平均延迟：$($parsedResult.Latency_Avg_ms)ms | TPS：$($parsedResult.TPS)" -ForegroundColor $colorInfo

            $testRecord = [PSCustomObject]@{
                DB = $db
                Clients = $clientNum
                Threads = $threadNum
                Run = $runIdx
                Latency_Avg = if ($parsedResult.Latency_Avg_ms -ne "") { try { [double]$parsedResult.Latency_Avg_ms } catch { $null } } else { $null }
                Init_Connection_Time = if ($parsedResult.Init_Connection_Time_ms -ne "") { try { [double]$parsedResult.Init_Connection_Time_ms } catch { $null } } else { $null }
                TPS = if ($parsedResult.TPS -ne "") { try { [double]$parsedResult.TPS } catch { $null } } else { $null }
                LogFile = Split-Path -Leaf $testLogPath
                DockerStats_Before = $statsBeforeTest
                DockerStats_After = $statsAfterTest
            }
            $allTestRecords += $testRecord

            $csvLine = "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}" -f `
                $db, $clientNum, $threadNum, $runIdx, `
                ($parsedResult.Latency_Avg_ms -replace ",",""), `
                ($parsedResult.Init_Connection_Time_ms -replace ",",""), `
                ($parsedResult.TPS -replace ",",""), `
                (Split-Path -Leaf $testLogPath), `
                ('"' + $statsBeforeTest + '"'), ('"' + $statsAfterTest + '"')
            $csvLine | Out-File -FilePath $resultsCsv -Append -Encoding utf8
            Write-Host "✅ 本次结果已写入逐次CSV：$resultsCsv" -ForegroundColor $colorSuccess

            if ($currentTask -ne $totalTestTasks) {
                Write-Host "`n⏳ 等待3秒后启动下一个测试任务..." -ForegroundColor $colorInfo
                Start-Sleep -Seconds 3
            }
            $currentTask++
        }
    }
}

# ==============================================
# 3. 生成聚合统计结果
# ==============================================
Write-Host "`n`n$separator" -ForegroundColor $colorTitle
Write-Host "【3/3】生成聚合统计结果" -ForegroundColor $colorTitle
Write-Host "$separator`n" -ForegroundColor $colorTitle

if ($allTestRecords.Count -eq 0) {
    Write-Host "❌ 无有效测试记录可统计！请检查测试执行过程是否正常。" -ForegroundColor $colorError
} else {
    Write-Host "ℹ️ 正在统计 $($allTestRecords.Count) 条测试数据（按数据库+客户端数分组）..." -ForegroundColor $colorInfo

    $groupedRecords = $allTestRecords | Group-Object -Property DB, Clients

    Write-Host "ℹ️ 聚合统计结果将保存至：$summaryCsv" -ForegroundColor $colorInfo
    "DB,Clients,Count,Mean_TPS,Median_TPS,Mean_Latency_ms,Median_Latency_ms" | Out-File -FilePath $summaryCsv -Encoding utf8

    foreach ($group in $groupedRecords) {
        $groupItems = $group.Group
        $groupDB = $groupItems[0].DB
        $groupClient = $groupItems[0].Clients
        $groupTestCount = $groupItems.Count

        $validTPS = $groupItems | Where-Object { $_.TPS -ne $null } | Select-Object -ExpandProperty TPS
        $validLatency = $groupItems | Where-Object { $_.Latency_Avg -ne $null } | Select-Object -ExpandProperty Latency_Avg

        $avgTPS = if ($validTPS.Count -gt 0) { [math]::Round(($validTPS | Measure-Object -Average).Average, 3) } else { "-" }
        $medianTPS = if ($validTPS.Count -gt 0) { [math]::Round((Get-Median $validTPS), 3) } else { "-" }
        $avgLatency = if ($validLatency.Count -gt 0) { [math]::Round(($validLatency | Measure-Object -Average).Average, 3) } else { "-" }
        $medianLatency = if ($validLatency.Count -gt 0) { [math]::Round((Get-Median $validLatency), 3) } else { "-" }

        $statsCsvLine = "{0},{1},{2},{3},{4},{5},{6}" -f `
            $groupDB, $groupClient, $groupTestCount, $avgTPS, $medianTPS, $avgLatency, $medianLatency
        $statsCsvLine | Out-File -FilePath $summaryCsv -Append -Encoding utf8

        Write-Host "`n📊 分组统计 | 数据库：$groupDB | 客户端数：$groupClient" -ForegroundColor $colorInfo
        Write-Host "   测试次数：$groupTestCount | 平均TPS：$avgTPS | 中位数TPS：$medianTPS" -ForegroundColor $colorInfo
        Write-Host "   平均延迟：$avgLatency ms | 中位数延迟：$medianLatency ms" -ForegroundColor $colorInfo
    }
    Write-Host "`n✅ 聚合统计完成！统计文件路径：$summaryCsv" -ForegroundColor $colorSuccess
}

# ==============================================
# 4. 测试完成最终汇总
# ==============================================
Write-Host "`n`n$separator" -ForegroundColor $colorSuccess
Write-Host "全部测试流程已完成！" -ForegroundColor $colorSuccess
Write-Host "$separator`n" -ForegroundColor $colorSuccess

Write-Host "📁 结果根目录（含所有日志+CSV）：$outdir" -ForegroundColor $colorSuccess
Write-Host "📄 逐次测试详情CSV：$resultsCsv（每条测试的原始指标）" -ForegroundColor $colorInfo
Write-Host "📄 聚合统计CSV：$summaryCsv（均值/中位数对比，便于分析）" -ForegroundColor $colorInfo
$null = Read-Host '按下Enter按键以退出'