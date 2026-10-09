"""Authenticated ingress from the NUT container's driver event bridge."""
import os
import time
from datetime import datetime
from typing import Dict, Literal
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from services.monitor import get_monitor

router = APIRouter()


class DriverFrame(BaseModel):
    variables: Dict[str, str] = Field(default_factory=dict)
    healthy: bool
    reason: Literal['STATUS', 'UPDATE', 'HEARTBEAT', 'DISCONNECTED']
    created_at: float


@router.post('/system/driver-event')
async def receive_driver_event(frame: DriverFrame):
    if os.environ.get('NUT_EVENT_BRIDGE') != 'true':
        raise HTTPException(503, 'Driver event bridge disabled')
    monitor = get_monitor()
    if not monitor:
        raise HTTPException(503, 'Monitor not ready')
    if not -5 <= time.time() - frame.created_at <= 30:
        raise HTTPException(422, 'Expired driver frame')
    if len(frame.variables) > 512 or any(len(k) > 256 or len(v) > 8192 for k, v in frame.variables.items()):
        raise HTTPException(422, 'Invalid driver frame size')
    monitor._driver_bridge_data = dict(frame.variables) if frame.healthy else {}
    if frame.reason == 'STATUS' and frame.healthy:
        monitor._driver_bridge_statuses.append((time.monotonic(), dict(frame.variables)))
    elif not frame.healthy:
        monitor._driver_bridge_statuses.clear()
    monitor._driver_bridge_seen = time.monotonic()
    monitor._driver_bridge_frames += 1
    enabled = monitor.config and monitor.config.event_driven_enabled and monitor.config.monitoring_mode in ('event_driven', 'hybrid')
    monitor._event_mode_active = bool(enabled and frame.healthy and frame.variables.get('ups.status'))
    monitor._last_update_time = datetime.now()
    if frame.reason != 'HEARTBEAT' or not frame.healthy:
        monitor._driver_bridge_wakeup.set()
    return {'accepted': True, 'event_mode_active': monitor._event_mode_active}
