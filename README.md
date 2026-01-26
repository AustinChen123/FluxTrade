# FluxTrade

FluxTrade is a high-performance, microservices-based automated cryptocurrency trading system. It is designed for low-latency data processing, flexible strategy execution, and real-time risk management.

🌍 **Documentation**: [繁體中文 README](README.zh-TW.md) | [English Developer Guide](docs/en/developer_guide.md) | [User Guide](docs/user_guide.md)

## ✨ Features

*   **🚀 Rust Core Data Engine**: Ultra-low latency WebSocket data ingestion and candlestick aggregation, ensuring strategies always run on the freshest data.
*   **🐍 Python Hot-Plug Strategies**: Write strategies in familiar Python with support for dynamic loading and automated backfilling—no system restart required.
*   **🛡️ Safety-First Risk Management**: Built-in forced market order protection and real-time balance checks (Redis-backed Risk Manager) to safeguard your capital.
*   **📊 Full Observability**: Integrated Streamlit dashboard providing real-time PnL monitoring, position tracking, and strategy status visualization.

## System Architecture

The system consists of three core services:

1.  **Rust Data Service**: 
    - Manages WebSocket connections to exchanges (Binance, Bybit, Backpack).
    - Standardizes real-time market data (Trades, Candles) and publishes to Redis Pub/Sub.
    - Implements high-performance candlestick aggregation.

2.  **Python Strategy Service**:
    - Subscribes to Redis data streams.
    - Runs the Strategy Engine for execution logic.
    - Performs risk checks (Risk Manager) and order management (Order Manager).
    - Supports backtesting and simulation modes.

3.  **Dashboard (Python/Streamlit)**:
    - Provides real-time market overview, trade history, and risk monitoring interface.
    - Used for visualizing strategy performance and system health.

## Configuration

This project uses a `.env` file for environment variable management.

1.  **Copy the template**:
    ```bash
    cp .env.example .env
    ```

2.  **Configuration details**:
    Open the `.env` file and fill in your settings:

    *   **Database (PostgreSQL)**
        *   `POSTGRES_USER`: Database username (Default: fluxtrade)
        *   `POSTGRES_PASSWORD`: Database password
        *   `POSTGRES_DB`: Database name
        *   `POSTGRES_HOST`: Host (Use localhost for local dev)
    
    *   **Cache (Redis)**
        *   `REDIS_HOST`: Redis host (Default: localhost)
        
    *   **Exchange Settings**
        *   `EXCHANGE_ID`: Target exchange (e.g., binance)
        *   `EXCHANGE_API_KEY`: Your API Key
        *   `EXCHANGE_SECRET`: Your API Secret
        *   `EXCHANGE_TESTNET`: Use testnet (true/false)

## Quick Start

We recommend using Docker Compose for a one-click deployment.

1.  **Start all services**
    ```bash
    docker-compose -f docker-compose.prod.yml up -d
    ```

2.  **Access the Dashboard**
    Open your browser and navigate to [http://localhost:8501](http://localhost:8501)

3.  **Stop services**
    ```bash
    docker-compose -f docker-compose.prod.yml down
    ```

For manual service startup instructions, please refer to the [User Guide](docs/user_guide.md).