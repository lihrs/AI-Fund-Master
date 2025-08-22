class Cache:
    """In-memory cache for API responses."""

    def __init__(self):
        self._prices_cache: dict[str, list[dict[str, any]]] = {}
        self._financial_metrics_cache: dict[str, list[dict[str, any]]] = {}
        self._line_items_cache: dict[str, list[dict[str, any]]] = {}
        self._insider_trades_cache: dict[str, list[dict[str, any]]] = {}
        self._company_news_cache: dict[str, list[dict[str, any]]] = {}

    def _merge_data(self, existing: list[dict] | None, new_data: list[dict], key_field: str) -> list[dict]:
        """Merge existing and new data, avoiding duplicates based on a key field."""
        if not existing:
            return new_data

        # Create a set of existing keys for O(1) lookup
        existing_keys = {item[key_field] for item in existing}

        # Only add items that don't exist yet
        merged = existing.copy()
        merged.extend([item for item in new_data if item[key_field] not in existing_keys])
        return merged

    def get_prices(self, cache_key: str) -> list[dict[str, any]] | None:
        """Get cached price data if available."""
        return self._prices_cache.get(cache_key)

    def set_prices(self, cache_key: str, data: list[dict[str, any]]):
        """Set price data to cache with exact key match."""
        self._prices_cache[cache_key] = data

    def get_financial_metrics(self, cache_key: str) -> list[dict[str, any]]:
        """Get cached financial metrics if available."""
        return self._financial_metrics_cache.get(cache_key)

    def set_financial_metrics(self, cache_key: str, data: list[dict[str, any]]):
        """Set financial metrics to cache with exact key match."""
        self._financial_metrics_cache[cache_key] = data

    def get_line_items(self, cache_key: str) -> list[dict[str, any]] | None:
        """Get cached line items if available."""
        return self._line_items_cache.get(cache_key)

    def set_line_items(self, cache_key: str, data: list[dict[str, any]]):
        """Set line items to cache with exact key match."""
        self._line_items_cache[cache_key] = data

    def get_insider_trades(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached insider trades if available."""
        return self._insider_trades_cache.get(ticker)

    def set_insider_trades(self, ticker: str, data: list[dict[str, any]]):
        """Append new insider trades to cache."""
        self._insider_trades_cache[ticker] = self._merge_data(self._insider_trades_cache.get(ticker), data, key_field="filing_date")  # Could also use transaction_date if preferred

    def get_company_news(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached company news if available."""
        return self._company_news_cache.get(ticker)

    def set_company_news(self, ticker: str, data: list[dict[str, any]]):
        """Append new company news to cache."""
        self._company_news_cache[ticker] = self._merge_data(self._company_news_cache.get(ticker), data, key_field="date")
    
    def clear_cache(self):
        """Clear all cached data."""
        cache_counts = {
            "prices": len(self._prices_cache),
            "financial_metrics": len(self._financial_metrics_cache),
            "line_items": len(self._line_items_cache),
            "insider_trades": len(self._insider_trades_cache),
            "company_news": len(self._company_news_cache)
        }
        
        self._prices_cache.clear()
        self._financial_metrics_cache.clear()
        self._line_items_cache.clear()
        self._insider_trades_cache.clear()
        self._company_news_cache.clear()
        
        total_cleared = sum(cache_counts.values())
        print(f"✓ API缓存已清空: {total_cleared} 个缓存项")
        print(f"  - 价格数据: {cache_counts['prices']} 项")
        print(f"  - 财务指标: {cache_counts['financial_metrics']} 项")
        print(f"  - 财务报表: {cache_counts['line_items']} 项")
        print(f"  - 内部交易: {cache_counts['insider_trades']} 项")
        print(f"  - 公司新闻: {cache_counts['company_news']} 项")
        
        return total_cleared
    
    def get_cache_info(self):
        """Get cache information."""
        return {
            "prices_count": len(self._prices_cache),
            "financial_metrics_count": len(self._financial_metrics_cache),
            "line_items_count": len(self._line_items_cache),
            "insider_trades_count": len(self._insider_trades_cache),
            "company_news_count": len(self._company_news_cache),
            "total_count": len(self._prices_cache) + len(self._financial_metrics_cache) + 
                          len(self._line_items_cache) + len(self._insider_trades_cache) + 
                          len(self._company_news_cache)
        }


# Global cache instance
_cache = Cache()


def get_cache() -> Cache:
    """Get the global cache instance."""
    return _cache
