# MOI's Proxy - AI Proxy with IP Rotation

A FastAPI-based proxy server that provides OpenAI-compatible API endpoints with IP rotation using WebScrapingAPI. This proxy allows you to generate custom API keys with configurable target URLs and API keys.

## Features

- **IP Rotation**: Automatically rotates between 5 different IP addresses using WebScrapingAPI
- **OpenAI-Compatible API**: Works with any OpenAI-compatible client (OpenAI, OpenRouter, Airforce, etc.)
- **Custom API Keys**: Generate API keys with custom target URLs and API keys
- **Rate Limiting**: Configurable RPM (requests per minute) and RPD (requests per day) limits
- **Real-time Logging**: WebSocket-based live log streaming
- **Admin Dashboard**: Web-based dashboard for managing API keys
- **Public Landing Page**: Modern cyberpunk-themed public interface for API key verification
- **Multi-Provider Support**: Works with OpenRouter, OpenAI, Airforce, and other OpenAI-compatible APIs

## How It Works

1. Clients send requests to your proxy using generated API keys
2. The proxy validates the API key and checks rate limits
3. Requests are forwarded through WebScrapingAPI with rotating IP sessions (1-5)
4. Each generated API key can have its own custom target URL and API key

---

## Netlify Deployment

### Quick Deploy to Netlify

1. **Push your code to GitHub**
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/your-username/your-repo.git
   git push -u origin main
   ```

2. **Deploy on Netlify**
   - Go to [Netlify Dashboard](https://app.netlify.com/)
   - Click **Add new site** → **Import an existing project**
   - Connect to GitHub and select your repository
   - Configure build settings:
     - **Build command**: `pip install -r requirements.txt`
     - **Publish directory**: `frontend`
   - Click **Deploy site**

3. **Configure Environment Variables**
   - In your Netlify site dashboard, go to **Site settings** → **Environment variables**
   - Add each variable listed below

4. **Set up Netlify Functions (for backend)**
   - Create a `netlify/functions` directory for serverless functions
   - Or use Netlify's proxy redirects in `netlify.toml`

### Environment Variables for Netlify

Copy and paste these into Netlify's Environment Variables section:

| Variable | Required | Description | Example Value |
|----------|----------|-------------|---------------|
| `WEBSCRAPINGAPI_ENABLED` | **Yes** | Enable IP rotation proxy | `true` |
| `WEBSCRAPINGAPI_KEY` | **Yes** | Your WebScrapingAPI API key | `BuNhUrNq6LtkGCHj6PeG1xlOrsWaUz9l` |
| `WEBSCRAPINGAPI_URL` | No | WebScrapingAPI endpoint | `https://api.webscrapingapi.com/v2` |
| `DEFAULT_TARGET_URL` | **Yes** | Default OpenAI-compatible API URL | `https://api.openai.com/v1` |
| `DEFAULT_TARGET_API_KEY` | **Yes** | Default API key for target | `sk-your-openai-api-key` |
| `ADMIN_PASSWORD` | **Yes** | Password for admin dashboard | `YourSecurePassword123!` |
| `DATABASE_PATH` | No | SQLite database path | `./data/proxy.db` |
| `HOST` | No | Server host address | `0.0.0.0` |
| `PORT` | No | Server port number | `3000` |
| `LOG_LEVEL` | No | Logging level | `INFO` |
| `CORS_ORIGINS` | No | Allowed CORS origins | `*` |

### Netlify Environment Variables - Quick Copy

```env
# Required Variables
WEBSCRAPINGAPI_ENABLED=true
WEBSCRAPINGAPI_KEY=BuNhUrNq6LtkGCHj6PeG1xlOrsWaUz9l
DEFAULT_TARGET_URL=https://api.openai.com/v1
DEFAULT_TARGET_API_KEY=sk-your-openai-api-key
ADMIN_PASSWORD=YourSecurePassword123!

# Optional Variables
WEBSCRAPINGAPI_URL=https://api.webscrapingapi.com/v2
DATABASE_PATH=./data/proxy.db
HOST=0.0.0.0
PORT=3000
LOG_LEVEL=INFO
CORS_ORIGINS=*
```

### netlify.toml Configuration

Create a `netlify.toml` file in your project root:

```toml
[build]
  publish = "frontend"
  command = "echo 'Static frontend only'"

[[redirects]]
  from = "/api/*"
  to = "YOUR_BACKEND_URL/:splat"
  status = 200
  force = true
```

> **Note**: Netlify is primarily for static hosting. For the full backend functionality, you'll need to:
> 1. Deploy the backend separately (Railway, Render, Fly.io, or a VPS)
> 2. Use the `netlify.toml` redirects to proxy API calls to your backend
> 3. Or convert to Netlify Functions (serverless)

---

## Zeabur Deployment

### Quick Deploy to Zeabur

1. **Push your code to GitHub**
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/your-username/your-repo.git
   git push -u origin main
   ```

2. **Deploy on Zeabur**
   - Go to [Zeabur Dashboard](https://zeabur.com/dashboard)
   - Click **New Project**
   - Select **Deploy from GitHub**
   - Choose your repository
   - Zeabur will auto-detect Python and deploy

3. **Configure Environment Variables**
   - In your Zeabur project, go to **Variables**
   - Add the required environment variables (see below)

4. **Your API will be available at:**
   ```
   https://your-app-name.zeabur.app/v1/chat/completions
   ```

### Environment Variables for Zeabur

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `WEBSCRAPINGAPI_ENABLED` | **Yes** | Enable IP rotation | `true` |
| `WEBSCRAPINGAPI_KEY` | **Yes** | WebScrapingAPI API key | `BuNhUrNq6LtkGCHj6PeG1xlOrsWaUz9l` |
| `WEBSCRAPINGAPI_URL` | No | WebScrapingAPI endpoint | `https://api.webscrapingapi.com/v2` |
| `DEFAULT_TARGET_URL` | **Yes** | Default target API URL | `https://api.openai.com/v1` |
| `DEFAULT_TARGET_API_KEY` | **Yes** | Default target API key | `sk-your-openai-api-key` |
| `ADMIN_PASSWORD` | **Yes** | Admin dashboard password | `your-secure-password` |
| `DATABASE_PATH` | No | SQLite database path | `./data/proxy.db` |
| `HOST` | No | Server host | `0.0.0.0` |
| `PORT` | No | Server port | `3000` |
| `LOG_LEVEL` | No | Log level | `INFO` |
| `CORS_ORIGINS` | No | CORS origins | `*` |

### Zeabur Example Configuration

```env
WEBSCRAPINGAPI_ENABLED=true
WEBSCRAPINGAPI_KEY=BuNhUrNq6LtkGCHj6PeG1xlOrsWaUz9l
DEFAULT_TARGET_URL=https://api.openai.com/v1
DEFAULT_TARGET_API_KEY=sk-your-openai-api-key
ADMIN_PASSWORD=MySecurePassword123!
LOG_LEVEL=INFO
CORS_ORIGINS=*
```

---

## All Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `WEBSCRAPINGAPI_ENABLED` | Yes | `false` | Set to `true` to enable IP rotation |
| `WEBSCRAPINGAPI_KEY` | Yes* | *(none)* | Your WebScrapingAPI key (*required if enabled) |
| `WEBSCRAPINGAPI_URL` | No | `https://api.webscrapingapi.com/v2` | WebScrapingAPI endpoint |
| `DEFAULT_TARGET_URL` | Yes | `https://api.openai.com/v1` | Default OpenAI-compatible API URL |
| `DEFAULT_TARGET_API_KEY` | Yes | *(none)* | Default API key for target API |
| `ADMIN_PASSWORD` | Yes | `admin123` | Admin dashboard password (**change this!**) |
| `DATABASE_PATH` | No | `./data/proxy.db` | SQLite database file path |
| `HOST` | No | `0.0.0.0` | Server bind address |
| `PORT` | No | `3000` | Server port |
| `LOG_LEVEL` | No | `INFO` | Logging level (DEBUG/INFO/WARNING/ERROR) |
| `CORS_ORIGINS` | No | `*` | Comma-separated allowed origins |

---

## Usage

### Using the Proxy API

Once deployed, use your generated API keys like this:

```python
import openai

client = openai.OpenAI(
    base_url="https://your-proxy-url.com/v1",
    api_key="your-generated-proxy-key"
)

response = client.chat.completions.create(
    model="gpt-4",
    messages=[{"role": "user", "content": "Hello!"}]
)
print(response.choices[0].message.content)
```

### Using with curl

```bash
curl -X POST "https://your-proxy-url.com/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-generated-proxy-key" \
  -d '{
    "model": "gpt-4",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

### Admin Dashboard

Access the admin dashboard at:
```
https://your-proxy-url.com/dashboard.html
```

Use the access code `10Diniru` to enter, then use your `ADMIN_PASSWORD` to authenticate.

---

## Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/your-repo.git
   cd your-repo
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Create a `.env` file**
   ```bash
   cp .env.example .env
   # Edit .env with your values
   ```

5. **Run the server**
   ```bash
   cd backend
   python proxy_server.py
   ```

6. **Access the application**
   - API: `http://localhost:3000/v1/chat/completions`
   - Public Page: `http://localhost:3000/`
   - Admin Dashboard: `http://localhost:3000/dashboard.html`

---

## Project Structure

```
The proxy openai airforce/
├── backend/
│   ├── config.py          # Configuration and environment variables
│   ├── database.py        # SQLite database operations
│   └── proxy_server.py    # Main FastAPI server
├── frontend/
│   ├── index.html         # Public landing page
│   ├── style.css          # Public page styles
│   ├── script.js          # Public page JavaScript
│   ├── dashboard.html     # Admin dashboard
│   ├── dashboard.css      # Admin styles
│   └── dashboard.js       # Admin JavaScript
├── data/
│   └── proxy.db           # SQLite database (auto-created)
├── requirements.txt       # Python dependencies
├── netlify.toml           # Netlify configuration
└── README.md              # This file
```

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/chat/completions` | POST | Chat completions (OpenAI-compatible) |
| `/v1/completions` | POST | Text completions |
| `/v1/embeddings` | POST | Generate embeddings |
| `/v1/models` | GET | List available models |
| `/api/keys` | GET/POST | Manage API keys (admin) |
| `/api/keys/{key_id}` | PUT/DELETE | Update/delete keys (admin) |
| `/api/usage` | GET | Get usage statistics |
| `/api/validate-key` | POST | Validate an API key |
| `/api/key-models/{api_key}` | GET | Get models for a specific key |

---

## License

MIT License - feel free to use and modify as needed.

## API Endpoints

### Proxy Endpoint

```
POST /v1/chat/completions
Authorization: Bearer <your-generated-api-key>
```

This endpoint is compatible with OpenAI's chat completions API.

### Admin Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/keys/generate` | POST | Generate a new API key |
| `/admin/keys/list` | GET | List all API keys |
| `/admin/keys/{key_id}/toggle` | PUT | Enable/disable an API key |
| `/admin/keys/{key_id}/limits` | PUT | Update rate limits and target settings |
| `/admin/keys/{key_id}` | DELETE | Delete an API key |
| `/admin/keys/{key_id}/refresh` | POST | Reset rate limit counters |
| `/admin/keys/{key_id}/schedule` | PUT | Set auto-refresh schedule |
| `/admin/keys/{key_id}/usage` | GET | Get usage statistics |
| `/admin/logs/stream` | WebSocket | Real-time log streaming |

### Health Check

```
GET /health
```

## Generating API Keys with Custom Targets

When generating a new API key, you can specify:

- **Name**: A friendly name for the key
- **Max RPM**: Maximum requests per minute
- **Max RPD**: Maximum requests per day
- **Custom Target URL**: Override the default target API URL
- **Custom Target API Key**: Override the default target API key

### Example: Generate Key via API

```bash
curl -X POST "https://your-app.zeabur.app/admin/keys/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "My Custom Key",
    "max_rpm": 60,
    "max_rpd": 1000,
    "target_url": "https://api.anthropic.com/v1",
    "target_api_key": "sk-ant-your-anthropic-key"
  }'
```

## IP Rotation

The proxy uses WebScrapingAPI to rotate between 5 different IP addresses. Each request is assigned a session ID (1-5) in a round-robin fashion, ensuring requests appear to come from different IP addresses.

```
Request 1 → Session 1 → IP A
Request 2 → Session 2 → IP B
Request 3 → Session 3 → IP C
Request 4 → Session 4 → IP D
Request 5 → Session 5 → IP E
Request 6 → Session 1 → IP A (rotation restarts)
```

### Automatic Bypass for Large Requests

WebScrapingAPI has a request body size limit. To prevent "502 request entity too large" errors, the proxy automatically bypasses WebScrapingAPI when the request body exceeds **50KB** (configurable via `MAX_WEBSCRAPING_BODY_SIZE` in `proxy_server.py`).

- **Small requests** (≤50KB): Routed through WebScrapingAPI with IP rotation
- **Large requests** (>50KB): Sent directly to the target API (no IP rotation)

This ensures that long conversations with extensive message history still work correctly, while shorter requests benefit from IP rotation.

## Local Development

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your environment variables
4. Run the server:
   ```bash
   python backend/proxy_server.py
   ```
5. Access the dashboard at `http://localhost:3000`

## Project Structure

```
The proxy openai airforce/
├── backend/
│   ├── config.py          # Configuration and environment variables
│   ├── database.py        # SQLite database operations
│   └── proxy_server.py    # FastAPI server and proxy logic
├── frontend/
│   ├── dashboard.html     # Admin dashboard HTML
│   ├── dashboard.css      # Dashboard styles
│   └── dashboard.js       # Dashboard JavaScript
├── data/
│   └── proxy.db           # SQLite database (auto-created)
├── requirements.txt       # Python dependencies
└── README.md              # This file
```

## Security Notes

- API keys are stored as SHA-256 hashes in the database
- The full API key is only shown once during generation
- Use HTTPS in production
- Set a strong `ADMIN_PASSWORD`
- Consider restricting `CORS_ORIGINS` in production

## License

MIT License