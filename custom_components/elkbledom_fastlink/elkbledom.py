import asyncio
import logging
import json
import os
from typing import Tuple, TypeVar, Callable, cast, Any
from bleak.backends.service import BleakGATTServiceCollection

# Совместимость с разными версиями bleak
try:
    from bleak.exc import BleakDBusError
except Exception:
    try:
        from bleak.exc import BleakError as BleakDBusError
    except Exception:
        class BleakDBusError(Exception):
            pass

from bleak_retry_connector import (
    BleakClientWithServiceCache,
    BleakNotFoundError,
    BLEAK_RETRY_EXCEPTIONS as BLEAK_EXCEPTIONS,
    establish_connection,
)
from homeassistant.components.bluetooth import async_ble_device_from_address
from .const import DEFAULT_BRIGHTNESS_MODE

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------
# BLE-названия и настройки
# ---------------------------------------------------------
NAME_ARRAY = ["ELK-BLEDDM", "ELK-BLE", "LEDBLE", "MELK-OG10", "MELK", "ELK-BULB2", "ELK-BULB", "ELK-LAMPL"]
WRITE_CHARACTERISTIC_UUIDS = ["0000fff3-0000-1000-8000-00805f9b34fb"] * 8
TURN_ON_CMD = [[0x7E, 0x00, 0x04, 0xF0, 0x00, 0x01, 0xFF, 0x00, 0xEF],
               [0x7E, 0x00, 0x04, 0xF0, 0x00, 0x01, 0xFF, 0x00, 0xEF],
               [0x7E, 0x00, 0x04, 0xF0, 0x00, 0x01, 0xFF, 0x00, 0xEF],
               [0x7e, 0x07, 0x04, 0xff, 0x00, 0x01, 0x02, 0x01, 0xef],
               [0x7E, 0x00, 0x04, 0xF0, 0x00, 0x01, 0xFF, 0x00, 0xEF],
               [0x7E, 0x00, 0x04, 0xF0, 0x00, 0x01, 0xFF, 0x00, 0xEF],
               [0x7E, 0x00, 0x04, 0xF0, 0x00, 0x01, 0xFF, 0x00, 0xEF],
               [0x7E, 0x00, 0x04, 0xF0, 0x00, 0x01, 0xFF, 0x00, 0xEF]]

TURN_OFF_CMD = [[0x7E, 0x00, 0x04, 0x00, 0x00, 0x00, 0xFF, 0x00, 0xEF],
                [0x7E, 0x00, 0x04, 0x00, 0x00, 0x00, 0xFF, 0x00, 0xEF],
                [0x7E, 0x00, 0x04, 0x00, 0x00, 0x00, 0xFF, 0x00, 0xEF],
                [0x7e, 0x07, 0x04, 0x00, 0x00, 0x00, 0x02, 0x01, 0xef],
                [0x7E, 0x00, 0x04, 0x00, 0x00, 0x00, 0xFF, 0x00, 0xEF],
                [0x7E, 0x00, 0x04, 0x00, 0x00, 0x00, 0xFF, 0x00, 0xEF],
                [0x7E, 0x00, 0x04, 0x00, 0x00, 0x00, 0xFF, 0x00, 0xEF],
                [0x7E, 0x00, 0x04, 0x00, 0x00, 0x00, 0xFF, 0x00, 0xEF]]

# Реалистичные диапазоны кельвинов для RGB-эмуляции
MIN_COLOR_TEMPS_K = [1800] * 8
MAX_COLOR_TEMPS_K = [7000] * 8

DEFAULT_ATTEMPTS = 3
BLEAK_BACKOFF_TIME = 0.25
STATE_FILE = "/config/.storage/elkbledom_fastlink_state.json"
RETRY_BACKOFF_EXCEPTIONS = (BleakDBusError,)
WrapFuncType = TypeVar("WrapFuncType", bound=Callable[..., Any])

# ---------------------------------------------------------
# Декоратор безопасных повторных попыток BLE
# ---------------------------------------------------------
def retry_bluetooth_connection_error(func: WrapFuncType) -> WrapFuncType:
    async def _async_wrap_retry(self: "BLEDOMInstance", *args, **kwargs):
        for attempt in range(DEFAULT_ATTEMPTS):
            try:
                return await func(self, *args, **kwargs)
            except BleakNotFoundError:
                raise
            except RETRY_BACKOFF_EXCEPTIONS as err:
                if attempt == DEFAULT_ATTEMPTS - 1:
                    LOGGER.error("%s: BLE retry exhausted: %s", self.name, err)
                    raise
                await asyncio.sleep(BLEAK_BACKOFF_TIME)
            except BLEAK_EXCEPTIONS as err:
                if attempt == DEFAULT_ATTEMPTS - 1:
                    LOGGER.error("%s: BLE exception: %s", self.name, err)
                    raise
    return cast(WrapFuncType, _async_wrap_retry)

# ---------------------------------------------------------
# Класс экземпляра устройства
# ---------------------------------------------------------
class BLEDOMInstance:
    def __init__(self, address, reset: bool, delay: int, hass) -> None:
        self.address = address
        self._reset = reset
        self._delay = delay
        self._hass = hass

        self._device = async_ble_device_from_address(hass, address)
        if not self._device:
            LOGGER.warning(
                "%s: Bluetooth device not currently available; starting offline until discovered",
                address,
            )

        self._client: BleakClientWithServiceCache | None = None
        self._is_connected = False
        self._connect_lock = asyncio.Lock()
        self._connect_event = asyncio.Event()  # Signal when connection completes
        self._connecting = False  # Track if reconnection is in progress
        self._reconnect_task_scheduled = False  # Prevent duplicate reconnection tasks
        self._ever_connected = False  # True only after first successful connection
        self._is_shutting_down = False  # Prevent new tasks during shutdown
        self._background_tasks: set = set()  # Track all background tasks for cleanup
        self._connection_callbacks: list = []  # Callbacks when connection state changes
        self._cached_services: BleakGATTServiceCollection | None = None
        self._write_uuid = None

        # Начальные значения
        self._is_on = False
        self._rgb_color: Tuple[int, int, int] = (255, 255, 255)
        self._brightness: int = 255
        self._color_temp_kelvin: int = 5000

        self._effect_speed: int = 16
        self._last_effect: int | None = None

        self._min_color_temp_kelvin = 1800
        self._max_color_temp_kelvin = 7000

        self._brightness_mode: str = DEFAULT_BRIGHTNESS_MODE

        self._color_mode = None
        self._model = None
        self._delayed_connect_time = 5

        self._detect_model()
        self._create_task(self._async_init_state())
        self._create_task(self._delayed_connect())
        LOGGER.debug("%s: BLEDOMInstance initialized", self.name)

    def _create_task(self, coro):
        """Create a background task and track it for cleanup."""
        if self._is_shutting_down:
            LOGGER.debug("%s: Skipping task creation during shutdown", self.name)
            return None
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def register_connection_callback(self, callback):
        """Register callback to be called when connection state changes."""
        if callback not in self._connection_callbacks:
            self._connection_callbacks.append(callback)

    def unregister_connection_callback(self, callback):
        """Unregister connection state change callback."""
        self._connection_callbacks.discard(callback) if isinstance(self._connection_callbacks, set) else None
        try:
            self._connection_callbacks.remove(callback)
        except ValueError:
            pass

    def _notify_connection_change(self):
        """Notify all registered callbacks about connection state change."""
        for callback in self._connection_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    self._create_task(callback())
                else:
                    callback()
            except Exception as e:
                LOGGER.debug("%s: Error in connection callback: %s", self.name, e)

    # ---------------------------------------------------------
    # JSON-состояние (асинхронно)
    # ---------------------------------------------------------
    def _load_state_sync(self):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data.get(self.address, {})
        except Exception as e:
            LOGGER.warning("Failed to load state for %s: %s", self.address, e)
        return {}

    async def _async_load_state(self):
        return await self._hass.async_add_executor_job(self._load_state_sync)

    def _save_state_sync(self, payload: dict | None = None):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            data: dict = {}
            if os.path.exists(STATE_FILE):
                try:
                    with open(STATE_FILE, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except json.JSONDecodeError:
                    data = {}

            if payload is None:
                payload = {
                    "is_on": self._is_on,
                    "rgb": self._rgb_color,
                    "brightness": self._brightness,
                    "color_temp": self._color_temp_kelvin,
                    "brightness_mode": self._brightness_mode,
                    "model": self._model,
                }

            data[self.address] = payload
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            LOGGER.error("Failed to save state for %s: %s", self.address, e)

    async def _async_save_state(self, payload: dict | None = None):
        await self._hass.async_add_executor_job(self._save_state_sync, payload)

    async def _async_init_state(self):
        LOGGER.debug("Loading saved state for %s from %s", self.address, STATE_FILE)
        state = await self._async_load_state()
        self._is_on = bool(state.get("is_on", False))
        self._rgb_color = tuple(state.get("rgb", (255, 255, 255)))  # type: ignore[arg-type]
        self._brightness = int(state.get("brightness", 255))
        self._color_temp_kelvin = int(state.get("color_temp", 5000))
        self._brightness_mode = str(state.get("brightness_mode", DEFAULT_BRIGHTNESS_MODE))
        self._model = str(state.get("model", self._model))

    # ---------------------------------------------------------
    # Режим яркости и переподключение
    # ---------------------------------------------------------
    async def apply_brightness_mode(self, mode: str):
        mode = (mode or DEFAULT_BRIGHTNESS_MODE).lower()
        if mode not in ("auto", "rgb", "native"):
            mode = DEFAULT_BRIGHTNESS_MODE
        if mode == self._brightness_mode:
            return
        self._brightness_mode = mode
        await self._async_save_state()
        await self.reconnect()

    async def reconnect(self):
        try:
            await self.stop()
        except Exception:
            pass
        await asyncio.sleep(1.0)
        await self._ensure_connected()
        LOGGER.info("%s: reconnected after mode change (%s)", self.name, self._brightness_mode)

    # ---------------------------------------------------------
    # Свойства
    # ---------------------------------------------------------
    @property
    def name(self):
        return self._device.name if self._device else self.address

    @property
    def is_on(self) -> bool:
        """Состояние включения устройства."""
        return getattr(self, "_is_on", False)

    @property
    def brightness(self) -> int:
        return getattr(self, "_brightness", 255)

    @property
    def rgb_color(self) -> tuple[int, int, int]:
        return getattr(self, "_rgb_color", (255, 255, 255))

    @property
    def color_temp_kelvin(self) -> int:
        return getattr(self, "_color_temp_kelvin", 5000)
    
    @property
    def color_mode(self):
        return getattr(self, "_color_mode", "rgb")
    
    @property
    def is_connected(self) -> bool:
        """Return whether the device is currently connected."""
        return self._is_connected and (self._client is not None and self._client.is_connected)

    # ---------------------------------------------------------
    # Подключение BLE
    # ---------------------------------------------------------
    async def _delayed_connect(self):
        """Attempt initial connection after a delay. Errors are non-fatal."""
        try:
            await asyncio.sleep(self._delayed_connect_time)
            if self._is_shutting_down:
                LOGGER.debug("%s: Skipping delayed connect during shutdown", self.name)
                return
            self._device = self._device or async_ble_device_from_address(self._hass, self.address)
            if not self._device:
                LOGGER.debug("%s: Initial connection skipped; device not found yet", self.address)
                self._is_connected = False
                return
            await self._ensure_connected()
        except asyncio.CancelledError:
            LOGGER.debug("%s: Delayed connect cancelled", self.name)
        except Exception as e:
            LOGGER.debug("%s: Initial connection failed (will retry on next command): %s", self.name, e)
            self._is_connected = False

    def _detect_model(self):
        device = self._device or async_ble_device_from_address(self._hass, self.address)
        if device:
            self._device = device
            dev_name = (device.name or "").lower()
        else:
            dev_name = ""

        for i, name in enumerate(NAME_ARRAY):
            if dev_name.startswith(name.lower()):
                self._turn_on_cmd = TURN_ON_CMD[i]
                self._turn_off_cmd = TURN_OFF_CMD[i]
                self._min_color_temp_kelvin = MIN_COLOR_TEMPS_K[i]
                self._max_color_temp_kelvin = MAX_COLOR_TEMPS_K[i]
                self._model = name
                return


    async def _ensure_connected(self):
        """Ensure device is connected, avoiding redundant connection attempts."""
        device = self._device or async_ble_device_from_address(self._hass, self.address)
        if not device:
            self._is_connected = False
            raise BleakNotFoundError(self.address)

        self._device = device
        self._detect_model()

        # Quick check before acquiring lock
        if self._client and self._client.is_connected:
            self._is_connected = True
            return
        
        # If already connecting, wait for it to complete
        if self._connecting:
            LOGGER.debug("%s: Connection already in progress, waiting...", self.name)
            try:
                # Wait with timeout for connection to complete
                await asyncio.wait_for(self._connect_event.wait(), timeout=15.0)
                if self._client and self._client.is_connected:
                    self._is_connected = True
                    LOGGER.debug("%s: Connected after waiting", self.name)
                    return
                else:
                    raise Exception("Connection attempt failed")
            except asyncio.TimeoutError:
                raise asyncio.TimeoutError(f"{self.name}: Timed out waiting for in-progress connection")
        
        self._connecting = True
        try:
            async with self._connect_lock:
                # Double-check after acquiring lock
                if self._client and self._client.is_connected:
                    self._is_connected = True
                    LOGGER.debug("%s: Already connected after lock acquired", self.name)
                    return
                
                try:
                    LOGGER.debug("%s: Establishing BLE connection", self.name)
                    client = await asyncio.wait_for(
                        establish_connection(
                            BleakClientWithServiceCache,
                            device,
                            device.name or self.address,
                            self._disconnected,
                            cached_services=self._cached_services,
                        ),
                        timeout=10.0
                    )
                    self._client = client
                    self._cached_services = client.services
                    for ch in WRITE_CHARACTERISTIC_UUIDS:
                        c = client.services.get_characteristic(ch)
                        if c:
                            self._write_uuid = c
                            break
                    self._is_connected = True
                    self._ever_connected = True  # Mark as successfully connected at least once
                    LOGGER.info("%s connected", self._device.name)
                    self._notify_connection_change()  # Notify entities that device is now available
                    # Restore power state in background without blocking entity creation
                    self._create_task(self._async_restore_power_state())
                except asyncio.TimeoutError:
                    LOGGER.debug("%s: Connection timeout (10s)", self._device.name)
                    self._is_connected = False
                    raise
                except Exception as e:
                    LOGGER.debug("%s: connection failed: %s", self._device.name, e)
                    self._is_connected = False
                    raise
        finally:
            self._connecting = False
            self._connect_event.set()  # Signal connection attempt completed

    async def _async_restore_power_state(self):
        """Restore power state after reconnection."""
        try:
            await asyncio.sleep(0.5)  # Small delay to allow device to be ready
            if self._is_on:
                LOGGER.debug("%s: Restoring power ON state", self.name)
                await self._write(self._turn_on_cmd)
            else:
                LOGGER.debug("%s: Restoring power OFF state", self.name)
                await self._write(self._turn_off_cmd)
        except Exception as e:
            LOGGER.debug("%s: Failed to restore power state: %s", self.name, e)

    def _disconnected(self, _client):
        """Handle disconnection callback from BLE client."""
        LOGGER.info("%s: Disconnected", self.name)
        self._is_connected = False
        self._notify_connection_change()  # Notify entities that device is now unavailable
        self._connect_event.clear()  # Clear event so waiting calls will wait again
        
        # Only attempt reconnection if we've ever successfully connected
        # (avoids multiple reconnection attempts during initialization)
        if not self._ever_connected:
            LOGGER.debug("%s: Never successfully connected; skipping automatic reconnection", self.name)
            return
        
        # Prevent duplicate reconnection tasks (callback may fire multiple times)
        if self._reconnect_task_scheduled:
            LOGGER.debug("%s: Reconnection task already scheduled, skipping duplicate", self.name)
            return
        
        self._reconnect_task_scheduled = True
        self._create_task(self._async_reconnect_with_retry())

    async def _async_reconnect_with_retry(self):
        """Attempt reconnection with exponential backoff and max attempts."""
        max_attempts = 4  # Fewer attempts with longer delays
        attempt = 0
        
        while attempt < max_attempts:
            if self._is_shutting_down:
                LOGGER.debug("%s: Reconnection cancelled during shutdown", self.name)
                self._reconnect_task_scheduled = False
                return
            
            if not await self._async_can_reconnect():
                # Device no longer reachable, wait for rediscovery
                LOGGER.debug("%s: Device not reachable, waiting for Bluetooth rediscovery", self.name)
                break
            
            attempt += 1
            # Longer delays: 5, 15, 30, 50 seconds - gives proxy time to cleanup
            delays = [5, 15, 30, 50]
            delay = delays[attempt - 1]
            
            LOGGER.debug("%s: Reconnection attempt %d/%d (waiting %ds first)", 
                        self.name, attempt, max_attempts, delay)
            
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                LOGGER.debug("%s: Reconnection sleep cancelled", self.name)
                self._reconnect_task_scheduled = False
                return
            
            try:
                await self._ensure_connected()
                LOGGER.debug("%s: Successfully reconnected", self.name)
                self._reconnect_task_scheduled = False
                return
            except Exception as e:
                LOGGER.debug("%s: Reconnection attempt %d failed: %s", self.name, attempt, e)
        
        # All attempts exhausted
        LOGGER.debug("%s: Reconnection exhausted after %d attempts, waiting for rediscovery", 
                   self.name, max_attempts)
        self._reconnect_task_scheduled = False

    async def _async_can_reconnect(self) -> bool:
        """Check if device is still reachable by Bluetooth proxy."""
        try:
            device = async_ble_device_from_address(self._hass, self.address)
            return device is not None
        except Exception:
            return False


    # ---------------------------------------------------------
    # BLE-команды
    # ---------------------------------------------------------
    @retry_bluetooth_connection_error
    async def _write(self, data: list[int]):
        await self._ensure_connected()
        await self._client.write_gatt_char(self._write_uuid, bytearray(data), False)
        LOGGER.debug("sending data to %s: %s", self.name, data)

    @retry_bluetooth_connection_error
    async def turn_on(self):
            await self._write(self._turn_on_cmd)
            #avoid changing to color when set to white mode
            if self._color_mode != "white":
                await asyncio.sleep(0.2)
                await self.set_color(self._rgb_color, self._brightness)
            self._is_on = True
            await self._async_save_state()

    @retry_bluetooth_connection_error
    async def turn_off(self):
        await self._async_save_state()
        await self._write(self._turn_off_cmd)
        self._is_on = False

    async def _write_native_brightness(self, percent: int):
        p = max(0, min(int(percent), 100))
        LOGGER.debug("color mode = %s with value (%d)", self._color_mode, p)
        if self._color_mode == "white" and self.name.lower().startswith("melk-og10"):           
            await self._write([0x7e, 0x07, 0x05, 0x01, p, 0xff, 0x02, 0x01, 0xef])
        else:
            await self._write([0x7E, 0x04, 0x01, p, 0xFF, 0x00, 0xFF, 0x00, 0xEF])

    @retry_bluetooth_connection_error
    async def set_brightness(self, value: int):
        self._brightness = max(1, min(int(value), 255))
        r, g, b = self._rgb_color
        percent = round(self._brightness * 100 / 255)
        mode = (self._brightness_mode or DEFAULT_BRIGHTNESS_MODE).lower()
        #LOGGER.debug("%s: brightness mode = %s with value (%s%%)", self.name, mode, percent)

        async def write_rgb_scaled():
            scale = self._brightness / 255.0
            rr, gg, bb = int(r * scale), int(g * scale), int(b * scale)
            LOGGER.debug("color mode = %s with value (%d)", self._color_mode, rr)
            if self._color_mode == "white" and self.name.lower().startswith("melk-og10"):           
                await self._write([0x7e, 0x07, 0x05, 0x01, int(rr), 0xff, 0x02, 0x01, 0xef])
            else:
                await self._write([0x7E, 0x00, 0x05, 0x03, rr, gg, bb, 0x00, 0xEF])

            
        async def write_native_then_rgb():
            LOGGER.debug("color mode = %s with value (%d)", self._color_mode, percent)
            if self._color_mode == "white" and self.name.lower().startswith("melk-og10"):           
                await self._write([0x7e, 0x07, 0x05, 0x01, percent, 0xff, 0x02, 0x01, 0xef])
            else:
                await self._write_native_brightness(percent)
                await asyncio.sleep(0.05)
                await self._write([0x7E, 0x00, 0x05, 0x03, r, g, b, 0x00, 0xEF])
        
        try:
            if mode == "rgb":
                await write_rgb_scaled()
            elif mode == "native":
                await self._write_native_brightness(percent)
            else:
                try:
                    await write_native_then_rgb()
                    LOGGER.debug("%s: brightness auto→native success (%s%%)", self.name, percent)
                except Exception as e:
                    LOGGER.warning("%s: native failed (%s), fallback to RGB: %s", self.name, percent, e)
                    await write_rgb_scaled()
        finally:
            await self._async_save_state()

    @retry_bluetooth_connection_error
    async def set_color(self, rgb: Tuple[int, int, int], brightness: int | None = None):
        if brightness is not None:
            self._brightness = max(1, min(int(brightness), 255))
        r, g, b = (max(0, min(255, c)) for c in rgb)
        self._rgb_color = (int(r), int(g), int(b))

        scale = self._brightness / 255.0
        rr, gg, bb = int(r * scale), int(g * scale), int(b * scale)
        await self._write([0x7E, 0x00, 0x05, 0x03, rr, gg, bb, 0x00, 0xEF])

        self._is_on = True
        await self._async_save_state()

    @retry_bluetooth_connection_error
    async def set_color_temp_kelvin(self, value: int, brightness: int | None = None):
        k_min, k_max = self._min_color_temp_kelvin, self._max_color_temp_kelvin
        k = max(k_min, min(int(value), k_max))
        self._color_temp_kelvin = k

        # Более реалистичные оттенки для тёплого и холодного света
        warm = (255, 138, 18)
        cool = (180, 220, 255)
        t = (k - k_min) / (k_max - k_min) if k_max > k_min else 1.0

        r = int(warm[0] + (cool[0] - warm[0]) * t)
        g = int(warm[1] + (cool[1] - warm[1]) * t)
        b = int(warm[2] + (cool[2] - warm[2]) * t)

        if brightness is not None:
            self._brightness = max(1, min(int(brightness), 255))

        await self.set_color((r, g, b), self._brightness)
        await self._async_save_state()

    @retry_bluetooth_connection_error
    async def set_effect(self, value: int):
        try:
            await self._ensure_connected()
            if value in (0x00, None):
                await self.set_color(self._rgb_color, self._brightness)
                self._last_effect = None
                return
            await self._write([0x7E, 0x00, 0x03, value, 0x03, 0x00, 0x00, 0x00, 0xEF])
            self._last_effect = value
        except Exception as e:
            LOGGER.error("%s: set_effect error: %s", self.name, e)
        finally: 
            await self._async_save_state()

    @retry_bluetooth_connection_error
    async def set_effect_speed(self, speed: int):
        s = max(1, min(int(speed), 31))
        self._effect_speed = s
        await self._write([0x7E, 0x00, 0x02, s, 0x03, 0x00, 0x00, 0x00, 0xEF])

    async def stop(self):
        """Stop the device and clean up all resources."""
        LOGGER.debug("%s: Stopping device", self.name)
        self._is_shutting_down = True
        self._reconnect_task_scheduled = False  # Prevent new reconnection tasks
        
        # Cancel all background tasks
        for task in list(self._background_tasks):
            if not task.done():
                LOGGER.debug("%s: Cancelling task %s", self.name, task.get_name())
                task.cancel()
        
        # Wait for all tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        
        # Save state and disconnect
        await self._async_save_state()
        if self._client and self._client.is_connected:
            try:
                await self._client.disconnect()
            except Exception as e:
                LOGGER.debug("%s: Error disconnecting: %s", self.name, e)
