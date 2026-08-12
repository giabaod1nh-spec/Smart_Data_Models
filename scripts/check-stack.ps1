$urls = @(
  'http://localhost:1026/version',
  'http://localhost:8123/ping',
  'http://localhost:8091/ready',
  'http://localhost:8092/ready',
  'http://localhost:8093/ready',
  'http://localhost:8095/ready',
  'http://localhost:8096/ready',
  'http://localhost:8081/api/system/health'
)
foreach ($url in $urls) {
  try { "$url -> $((Invoke-WebRequest $url -UseBasicParsing).StatusCode)" }
  catch { "$url -> FAIL: $($_.Exception.Message)" }
}
