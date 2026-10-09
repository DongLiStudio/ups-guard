"""Select native driver events, with opt-in NUT polling during stream failure."""
import time


async def read_driver_variables(monitor):
    """Only a fresh, healthy event snapshot suppresses NUT network reads."""
    now = time.monotonic()
    healthy = now - monitor._driver_bridge_seen < 45 and bool(
        monitor._driver_bridge_data.get('ups.status'))
    monitor._event_mode_active = healthy
    while monitor._driver_bridge_statuses and now - monitor._driver_bridge_statuses[0][0] > 25:
        monitor._driver_bridge_statuses.popleft()
    if healthy:
        if monitor._driver_bridge_statuses:
            return monitor._driver_bridge_statuses.popleft()[1]
        return dict(monitor._driver_bridge_data)
    monitor._driver_bridge_statuses.clear()
    if monitor.config.event_driven_fallback:
        # list_vars handles reconnects and raises on stale/unavailable UPS data.
        # Never substitute the old event snapshot when this query fails.
        return await monitor.nut_client.list_vars()
    return {}
