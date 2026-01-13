# Grafana Dashboard Setup

This guide helps you set up Grafana to visualize your time series data from the Gold Layer.

## Architecture

```
Gold Layer (Parquet) → PostgreSQL → Grafana → Dashboards
```

## Prerequisites

1. PostgreSQL running with `pendler_pilot` table populated
2. Docker Desktop installed
3. Data in PostgreSQL (run `publish_to_postgres.py` first)

## Quick Start

### 1. Set Environment Variable

Create a `.env` file in the `grafana/` directory (or use the main project `.env`):

```env
POSTGRES_PASSWORD=your_postgres_password_here
```

### 2. Start Grafana

```powershell
cd grafana
docker-compose up -d
```

### 3. Access Grafana

- Open browser: http://localhost:3000
- Login:
  - Username: `admin`
  - Password: `admin` (change on first login)

### 4. Verify Data Source

1. Go to **Configuration** → **Data Sources**
2. You should see "PostgreSQL" already configured
3. Click "Test" to verify connection

## Example Time Series Queries

### 1. Traffic Congestion Over Time

```sql
SELECT 
  time_bucket as time,
  meta_city,
  AVG(grid_0_0_congestion) as avg_congestion
FROM pendler_pilot
WHERE time_bucket >= NOW() - INTERVAL '7 days'
  AND grid_0_0_congestion IS NOT NULL
GROUP BY time_bucket, meta_city
ORDER BY time_bucket
```

### 2. Rail Stress Index (Delays > 5 min)

```sql
SELECT 
  time_bucket as time,
  meta_city,
  rail_stress_index * 100 as stress_percentage
FROM pendler_pilot
WHERE time_bucket >= NOW() - INTERVAL '24 hours'
  AND rail_stress_index IS NOT NULL
ORDER BY time_bucket
```

### 3. Weather vs Traffic Correlation

```sql
SELECT 
  time_bucket as time,
  meta_city,
  AVG(grid_0_0_rain) as rainfall_mm,
  AVG(grid_0_0_congestion) as congestion_ratio
FROM pendler_pilot
WHERE time_bucket >= NOW() - INTERVAL '7 days'
  AND grid_0_0_rain IS NOT NULL
  AND grid_0_0_congestion IS NOT NULL
GROUP BY time_bucket, meta_city
ORDER BY time_bucket
```

### 4. Bus vs Rail Delays Comparison

```sql
SELECT 
  time_bucket as time,
  meta_city,
  bus_avg_delay / 60.0 as bus_delay_minutes,
  rail_avg_delay / 60.0 as rail_delay_minutes
FROM pendler_pilot
WHERE time_bucket >= NOW() - INTERVAL '24 hours'
  AND bus_avg_delay IS NOT NULL
  AND rail_avg_delay IS NOT NULL
ORDER BY time_bucket
```

### 5. Hourly Patterns

```sql
SELECT 
  hour_of_day,
  meta_city,
  AVG(grid_0_0_congestion) as avg_congestion,
  AVG(rail_stress_index) as avg_rail_stress
FROM pendler_pilot
WHERE time_bucket >= NOW() - INTERVAL '30 days'
GROUP BY hour_of_day, meta_city
ORDER BY hour_of_day, meta_city
```

## Creating Dashboards

### Step 1: Create New Dashboard

1. Click **+** → **Create Dashboard**
2. Click **Add visualization**

### Step 2: Configure Panel

1. Select **PostgreSQL** data source
2. Choose **Time series** visualization
3. Paste one of the example queries above
4. Configure:
   - **Format as**: Time series
   - **Time column**: `time`
   - **Value columns**: Select your metrics

### Step 3: Panel Settings

- **Title**: Descriptive name
- **Unit**: Choose appropriate unit (percent, seconds, etc.)
- **Legend**: Show city names
- **Thresholds**: Add alert thresholds if needed

## Recommended Dashboard Panels

1. **Traffic Congestion Heatmap** - Show congestion by hour/day
2. **Rail Stress Index** - Percentage of trains delayed > 5 min
3. **Weather Impact** - Rainfall vs Congestion correlation
4. **City Comparison** - Side-by-side metrics for different cities
5. **Peak Hours Analysis** - Congestion patterns by hour
6. **Delay Trends** - Bus vs Rail delay comparison over time

## Troubleshooting

### Can't connect to PostgreSQL

- Make sure PostgreSQL is running: `Get-Service postgresql*`
- Check connection from host: `psql -h localhost -U traffic_user -d traffic_lake`
- Update `postgres.yml` if using different host/port

### No data showing

- Verify data exists: `SELECT COUNT(*) FROM pendler_pilot;`
- Check time range in query matches your data
- Ensure `publish_to_postgres.py` has run successfully

### Performance Issues

- Add indexes on `time_bucket` and `meta_city`:
  ```sql
  CREATE INDEX idx_time_bucket ON pendler_pilot(time_bucket);
  CREATE INDEX idx_city ON pendler_pilot(meta_city);
  ```

## Stopping Grafana

```powershell
cd grafana
docker-compose down
```

To remove all data:
```powershell
docker-compose down -v
```


