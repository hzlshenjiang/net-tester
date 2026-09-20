# 网络测速工具 (NetTester)

绿色版网络测速工具，针对国内网络环境优化。

## 特点

- 一键测速，自动选择最优 CDN 节点
- 多源下载测速（华为云/腾讯），取最快结果
- Cloudflare 上传测速端点
- TCP 连接延迟测试
- DPI 分辨率自适应
- 防呆超时保护，网络不畅时自动跳过
- 不写日志，仅本次运行内显示数据

## 使用

双击 `NetTester.exe` 即可运行，无需安装。

## 测速方案

| 项目 | 方案 | 说明 |
|------|------|------|
| 下载 | 华为云镜像 + 腾讯 CDN | 20-30MB 文件，多源取最快 |
| 上传 | Cloudflare `__up` | 5MB POST 数据 |
| 延迟 | TCP 连接华为云 | 5 次取平均 |

## 构建

```bash
python -m PyInstaller --onefile --windowed --name NetTester --icon icon.ico --add-data "icon.ico;." net_tester.py
```

## License

MIT
