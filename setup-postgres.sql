-- PostgreSQL Setup Script for WS2526-DADP Project
-- Run this in psql or pgAdmin Query Tool

-- Create database
CREATE DATABASE traffic_lake;

-- Create user (replace 'your_password_here' with a strong password)
CREATE USER traffic_user WITH PASSWORD 'your_password_here';

-- Grant privileges on database
GRANT ALL PRIVILEGES ON DATABASE traffic_lake TO traffic_user;

-- Connect to the new database
\c traffic_lake

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO traffic_user;

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO traffic_user;

-- Make traffic_user the owner (optional, but ensures full access)
ALTER SCHEMA public OWNER TO traffic_user;

