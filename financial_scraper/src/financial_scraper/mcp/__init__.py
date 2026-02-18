"""MCP server for financial_scraper - exposes scraping tools over Model Context Protocol."""

from .server import start_server

__all__ = ["start_server"]
