# Py Lite Chat (File Storage, No DB)

一个基于 FastAPI + 单文件存储的轻量聊天系统。

## 主要特性

- 首次打开网页即可初始化系统（不依赖 `.env`）
- 本地 JSON 存储（默认 `./data/chat.json`）
- 多用户隔离：用户只能看到自己的对话
- 管理员账号初始化：用户名固定为 `admin`，密码由首次初始化时设置
- 普通用户无法自行注册或修改密码，只能由管理员在网页里创建/重置
- Agent 支持私有/公共：
  - 普通用户：只能管理自己的 Agent
  - 管理员：可将自己创建的 Agent 设置为公共
  - 公共 Agent 所有用户可见
- 内置公共 Agent：`运维专家`
- 管理员可设置消息自动清理时间（小时）
- 普通用户仅可选择模型，不显示模型请求地址和密钥

## 运行

```bash
cd py-lite-chat
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
# source .venv/bin/activate

pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

浏览器打开：

```text
http://127.0.0.1:8000
```

## 首次初始化

首次访问会进入“首次初始化”页面，需要填写：

- 应用标题（可选）
- 应用副标题（可选）
- 管理员密码（账号固定为 `admin`）
- 默认模型（可选）

初始化完成后自动登录管理员。

## 数据文件

- 默认路径：`./data/chat.json`
- 可通过环境变量覆盖：

```bash
APP_DATA_PATH=/absolute/or/relative/path/chat.json
```

> 如果使用相对路径，会相对于项目目录解析。
