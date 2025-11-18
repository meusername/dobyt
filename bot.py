import csv
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta
from decimal import Decimal

import ccxt
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

load_dotenv()


class KellyCapitalManagement:
    def __init__(self, total_capital=Decimal("1000")):
        self.total_capital = total_capital
        self.risk_free_rate = Decimal("0.02")
        self.trade_history = []

    def calculate_position_size(
        self, win_rate, avg_win_pct, avg_loss_pct, max_risk=Decimal("0.1")
    ):
        """Консервативный критерий Келли"""
        if avg_loss_pct == 0 or avg_win_pct == 0:
            return Decimal("0.02")

        w = float(win_rate)
        b = float(avg_win_pct / abs(avg_loss_pct))

        kelly_f = (w * b - (1 - w)) / b if b != 0 else 0.01

        conservative_f = max(0.01, kelly_f * 0.25)
        conservative_f = min(conservative_f, float(max_risk))

        position_size = self.total_capital * Decimal(str(conservative_f))
        return position_size

    def update_trade_history(self, trade_result):
        """Обновляем историю для расчета параметров"""
        self.trade_history.append(trade_result)
        if len(self.trade_history) > 100:
            self.trade_history.pop(0)

    def get_trade_statistics(self):
        """Статистика для расчета Келли"""
        if len(self.trade_history) < 5:
            return Decimal("0.55"), Decimal("0.05"), Decimal("0.025")

        try:
            wins = [t for t in self.trade_history if t["pnl"] > 0]
            losses = [t for t in self.trade_history if t["pnl"] < 0]

            if not wins or not losses:
                return Decimal("0.55"), Decimal("0.05"), Decimal("0.025")

            win_rate = Decimal(str(len(wins) / len(self.trade_history)))
            avg_win = Decimal(str(sum(t["pnl_pct"] for t in wins) / len(wins)))
            avg_loss = Decimal(
                str(abs(sum(t["pnl_pct"] for t in losses) / len(losses)))
            )

            return win_rate, avg_win, avg_loss
        except:
            return Decimal("0.55"), Decimal("0.05"), Decimal("0.025")


class SmartOrderManager:
    def __init__(self, exchange):
        self.exchange = exchange

    def execute_smart_buy(self, symbol, amount_usdt, max_slippage=Decimal("0.005")):
        """
        Быстрая покупка для малого депозита.
        Использует лимитный ордер с запасом цены (Market-like).
        ИСПРАВЛЕНО: Корректный расчет объема во избежание Insufficient Balance.
        """
        try:
            # 1. Получаем актуальный стакан
            orderbook = self.exchange.fetch_order_book(symbol, limit=5)
            best_ask = float(orderbook["asks"][0][0])

            # 2. Рассчитываем цену покупки с запасом 0.5% (гарантия исполнения)
            buy_price = best_ask * 1.005

            # 3. Рассчитываем количество ПРАВИЛЬНО
            # Сначала уменьшаем сумму USDT на 1% (комиссия + запас на округление)
            usable_usdt = float(amount_usdt) * 0.99

            # Делим доступные деньги на ЦЕНУ ОРДЕРА (buy_price), а не на best_ask
            raw_quantity = usable_usdt / buy_price

            # 4. Приводим к точности биржи
            symbol_precision = self.exchange.market(symbol)

            # Проверка минимальной стоимости (Cost limits)
            min_cost = symbol_precision["limits"]["cost"]["min"]
            # Также проверяем min amount, если он есть
            min_amount = symbol_precision["limits"]["amount"]["min"]

            if min_cost and usable_usdt < min_cost:
                logger.warning(
                    f"⚠️ Сумма {usable_usdt:.2f} меньше минимума по стоимости {min_cost} для {symbol}"
                )
                return False

            if min_amount and raw_quantity < min_amount:
                logger.warning(
                    f"⚠️ Кол-во {raw_quantity} меньше минимума {min_amount} для {symbol}"
                )
                return False

            # Округление цены и количества методами ccxt
            price_final = self.exchange.price_to_precision(symbol, buy_price)
            amount_final = self.exchange.amount_to_precision(symbol, raw_quantity)

            logger.info(
                f"🛒 Попытка покупки {symbol}: {amount_final} шт. по цене {price_final} (Сумма: {usable_usdt:.2f})"
            )

            # 5. Создаем ордер
            order = self.exchange.create_order(
                symbol=symbol,
                type="limit",
                side="buy",
                amount=amount_final,
                price=price_final,
            )

            logger.info(f"✅ Ордер создан: {order['id']}")

            # 6. Быстрый мониторинг (ждем 5 секунд макс)
            return self.monitor_order_execution(order["id"], symbol, timeout=5)

        except Exception as e:
            logger.error(f"❌ Ошибка Smart Buy для {symbol}: {e}")
            return False

    def execute_smart_sell(
        self, symbol, quantity, current_price=None, max_slippage=Decimal("0.005")
    ):
        """
        Умная продажа с жесткой проверкой лимитов и обработкой пыли.
        """
        try:
            # 1. Получаем баланс
            base_currency = symbol.split("/")[0]
            try:
                balance = self.exchange.fetch_balance()
            except:
                return False  # Ошибка сети

            available = 0
            if "free" in balance and base_currency in balance["free"]:
                available = float(balance["free"][base_currency])

            # Если баланса нет на бирже, но он есть в БД - это рассинхрон.
            # Возвращаем True, чтобы бот удалил запись из БД.
            if available <= 0:
                logger.warning(f"⚠️ Баланс {symbol} на бирже 0. Удаляем из БД.")
                return True

            # 2. Проверка на пыль (Минимальная стоимость)
            # Получаем текущую цену, если не передана
            if current_price is None:
                ticker = self.exchange.fetch_ticker(symbol)
                current_price = float(ticker["last"])

            estimated_value = available * float(current_price)

            # ЕСЛИ СТОИМОСТЬ МЕНЬШЕ $2 - ЭТО ПЫЛЬ. ПРОДАТЬ НЕЛЬЗЯ.
            # Мы возвращаем True, чтобы система считала сделку "завершенной" (и удалила из БД)
            # но фактически мы ничего не продаем.
            if estimated_value < 2.0:
                logger.warning(
                    f"🧹 Пыль {symbol}: ${estimated_value:.2f} < $2. Пропуск продажи, удаление из учета."
                )
                return True

            # 3. Округляем ВНИЗ (truncate)
            amount_final = self.exchange.amount_to_precision(symbol, available)

            # Fix: иногда amount_to_precision округляет вверх
            if float(amount_final) > available:
                amount_final = self.exchange.amount_to_precision(
                    symbol, available * 0.999
                )

            # 4. Цена продажи
            orderbook = self.exchange.fetch_order_book(symbol, limit=5)
            best_bid = float(orderbook["bids"][0][0])
            sell_price = best_bid * 0.995  # -0.5% для гарантии
            price_final = self.exchange.price_to_precision(symbol, sell_price)

            logger.info(f"🔻 Продажа {symbol}: {amount_final} по {price_final}")

            order = self.exchange.create_order(
                symbol=symbol,
                type="limit",
                side="sell",
                amount=amount_final,
                price=price_final,
            )

            return self.monitor_order_execution(order["id"], symbol, timeout=10)

        except Exception as e:
            logger.error(f"❌ Ошибка Smart Sell для {symbol}: {e}")
            return False

    def monitor_order_execution(self, order_id, symbol, timeout=5):
        """
        Быстрый мониторинг исполнения с фиксом для Bybit API.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # ФИКС: Добавляем 'acknowledged': True, чтобы искать в любой истории
                # Или ловим ошибку, если ордер уже улетел в историю
                try:
                    order = self.exchange.fetch_order(order_id, symbol)
                except Exception as e:
                    # Если ошибка Bybit про "last 500 orders", пробуем считать это успехом,
                    # если баланс изменился, но надежнее проверить fetchClosedOrders
                    err_str = str(e).lower()
                    if "order does not exist" in err_str or "not found" in err_str:
                        # Возможно уже исполнился и ушел в архив
                        pass
                    elif "access an order" in err_str:
                        # Ошибка Bybit API, игнорируем и ждем
                        pass

                    # Альтернативная проверка: если ордера нет в открытых, значит он исполнен или отменен
                    open_orders = self.exchange.fetch_open_orders(symbol)
                    is_open = any(o["id"] == str(order_id) for o in open_orders)

                    if not is_open:
                        # Если его нет в открытых через 1-2 сек после создания - скорее всего исполнен
                        if time.time() - start_time > 2:
                            logger.info(
                                f"✨ Ордер {symbol} не найден в открытых (считаем исполненным)"
                            )
                            return True

                    time.sleep(1)
                    continue

                status = order["status"]

                if status == "closed":
                    logger.info(f"✨ Ордер {symbol} полностью исполнен")
                    return True
                elif status == "canceled":
                    logger.warning(f"⚠️ Ордер {symbol} отменен")
                    return False
                elif status == "open":
                    # Если частично исполнен, ждем дальше
                    filled = float(order.get("filled", 0))
                    if filled > 0 and time.time() - start_time > (timeout - 1):
                        logger.info(f"✨ Ордер {symbol} частично исполнен ({filled})")
                        return True

                time.sleep(0.5)  # Короткая пауза
            except Exception as e:
                logger.error(f"Ошибка мониторинга: {e}")
                time.sleep(1)

        # Если время вышло и ордер не исполнен - отменяем
        try:
            logger.warning(f"⏱ Таймаут ордера {symbol}. Отмена...")
            self.exchange.cancel_order(order_id, symbol)
            return False
        except Exception as e:
            # Если ошибка "Order does not exist", значит он успел исполниться
            if "does not exist" in str(e) or "not found" in str(e):
                logger.info(f"✨ Ордер {symbol} успел исполниться перед отменой")
                return True
            logger.error(f"Не удалось отменить ордер: {e}")

        return False


class PerformanceAnalytics:
    def __init__(self, log_file="trades.csv"):
        self.trade_history = []
        self.log_file = log_file

        if os.path.exists(self.log_file):
            self.load_from_csv()

    def add_trade(self, trade_data):
        """Добавляем сделку в историю"""
        trade_record = {
            "timestamp": datetime.now(),
            "symbol": trade_data["symbol"],
            "side": trade_data["side"],
            "quantity": float(trade_data["quantity"]),
            "entry_price": float(trade_data["entry_price"]),
            "exit_price": float(trade_data.get("exit_price", 0)),
            "pnl": float(trade_data.get("pnl", 0)),
            "pnl_pct": float(trade_data.get("pnl_pct", 0)),
            "commission": float(trade_data.get("commission", 0)),
        }
        self.trade_history.append(trade_record)
        self.save_to_csv(trade_record)

    def save_to_csv(self, trade_record):
        """Сохраняем сделку в CSV"""
        file_exists = os.path.isfile(self.log_file)
        with open(self.log_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(
                    [
                        "timestamp",
                        "symbol",
                        "side",
                        "quantity",
                        "entry_price",
                        "exit_price",
                        "pnl",
                        "pnl_pct",
                        "commission",
                    ]
                )
            writer.writerow(
                [
                    trade_record["timestamp"],
                    trade_record["symbol"],
                    trade_record["side"],
                    trade_record["quantity"],
                    trade_record["entry_price"],
                    trade_record["exit_price"],
                    trade_record["pnl"],
                    trade_record["pnl_pct"],
                    trade_record["commission"],
                ]
            )

    def load_from_csv(self):
        """Загружаем историю из CSV"""
        try:
            with open(self.log_file, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.trade_history.append(
                        {
                            "timestamp": datetime.strptime(
                                row["timestamp"], "%Y-%m-%d %H:%M:%S.%f"
                            ),
                            "symbol": row["symbol"],
                            "side": row["side"],
                            "quantity": float(row["quantity"]),
                            "entry_price": float(row["entry_price"]),
                            "exit_price": float(row["exit_price"]),
                            "pnl": float(row["pnl"]),
                            "pnl_pct": float(row["pnl_pct"]),
                            "commission": float(row["commission"]),
                        }
                    )
        except Exception as e:
            logger.error(f"Ошибка загрузки истории: {e}")

    def calculate_advanced_metrics(self):
        """Расчет продвинутых метрик"""
        if len(self.trade_history) < 5:
            return self.get_default_metrics()

        try:
            df = pd.DataFrame(self.trade_history)
            returns = df["pnl_pct"]
            total_return = (1 + returns).prod() - 1
            avg_return = returns.mean()
            std_return = returns.std()

            sharpe = (avg_return - 0.02) / std_return if std_return > 0 else 0

            cumulative = (1 + returns).cumprod()
            running_max = cumulative.expanding().max()
            drawdown = (cumulative - running_max) / running_max
            max_drawdown = drawdown.min()

            win_rate = (returns > 0).mean()

            gross_profit = returns[returns > 0].sum()
            gross_loss = abs(returns[returns < 0].sum())
            profit_factor = (
                gross_profit / gross_loss if gross_loss > 0 else float("inf")
            )

            calmar = -total_return / max_drawdown if max_drawdown < 0 else float("inf")

            return {
                "total_return": total_return,
                "sharpe_ratio": sharpe,
                "max_drawdown": max_drawdown,
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "calmar_ratio": calmar,
                "total_trades": len(self.trade_history),
                "avg_trade_return": avg_return,
            }
        except Exception as e:
            logger.error(f"Ошибка расчета метрик: {e}")
            return self.get_default_metrics()

    def get_default_metrics(self):
        """Метрики по умолчанию"""
        return {
            "total_return": 0,
            "sharpe_ratio": 0,
            "max_drawdown": 0,
            "win_rate": 0,
            "profit_factor": 0,
            "calmar_ratio": 0,
            "total_trades": 0,
            "avg_trade_return": 0,
        }

    def generate_performance_report(self):
        """Генерация отчета о производительности"""
        metrics = self.calculate_advanced_metrics()

        report = f"""
📊 ОТЧЕТ О ПРОИЗВОДИТЕЛЬНОСТИ
{"=" * 50}
Общая доходность: {metrics["total_return"]:.2%}
Коэффициент Шарпа: {metrics["sharpe_ratio"]:.2f}
Максимальная просадка: {metrics["max_drawdown"]:.2%}
Винрейт: {metrics["win_rate"]:.2%}
Фактор прибыли: {metrics["profit_factor"]:.2f}
Коэффициент Калмара: {metrics["calmar_ratio"]:.2f}
Всего сделок: {metrics["total_trades"]}
Средняя доходность сделки: {metrics["avg_trade_return"]:.2%}
{"=" * 50}
        """

        logger.info(report)
        return report


class BybitSpotBot:
    def __init__(self):
        self.exchange = ccxt.bybit(
            {
                "apiKey": os.getenv("BYBIT_API_KEY"),
                "secret": os.getenv("BYBIT_API_SECRET"),
                "enableRateLimit": True,
                "sandbox": False,
                "rateLimit": 100,
                "options": {"defaultType": "spot"},
            }
        )

        try:
            markets = self.exchange.load_markets()
            logger.info(f"✅ Успешно подключено к Bybit. Доступно пар: {len(markets)}")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Bybit: {e}")
            raise

        # 🔴 ИСПРАВЛЕННЫЕ ПАРАМЕТРЫ ДЛЯ РЕАЛЬНОГО КАПИТАЛА
        self.total_capital = Decimal("20")  # Подстраиваем под ваш баланс
        self.kelly_manager = KellyCapitalManagement(self.total_capital)
        self.performance_analytics = PerformanceAnalytics()
        self.smart_order_manager = SmartOrderManager(self.exchange)
        self.risk_per_trade = Decimal("0.05")  # 5% риска на сделку

        # Оптимальные интервалы
        self.rebalance_interval = 300  # 5 минут
        self.tickers_cache_ttl = 60

        # 🔴 ПАРАМЕТРЫ ДЛЯ МАЛОГО КАПИТАЛА
        self.max_positions = 2  # Максимум 2 позиции
        self.min_position_size = Decimal("5")  # Минимум $5
        self.max_position_size = Decimal("15")  # Максимум $15
        self.reserve_cash = Decimal("2")  # Резерв $2

        # Адаптивные стопы
        self.initial_stop_atr = Decimal("2.0")
        self.take_profit_atr = Decimal("3.0")  # Уменьшили TP для более частых сделок

        # Кэш для ATR
        self.atr_cache = {}
        self.atr_cache_ttl = 3600

        # Базовые параметры
        self.min_order = Decimal("5")
        self.commission = Decimal("0.001")
        self.position_size = Decimal("10")  # Для обратной совместимости
        self.STABLECOINS = [
            "USDC",
            "TUSD",
            "FDUSD",
            "USDD",
            "BUSD",
            "DAI",
            "PAX",
            "GUSD",
        ]

        # Защитные параметры
        self.stop_loss = Decimal("0.98")
        self.take_profit = Decimal("1.03")
        self.trailing_stop = Decimal("0.985")
        self.max_hold_hours = 6

        # Трейлинг-стоп
        self.trailing_stop_max_prices = {}

        # Параметры БД
        self.db_config = {
            "host": os.getenv("DB_HOST", "127.0.0.1"),
            "database": os.getenv("DB_NAME", "dobyt"),
            "user": os.getenv("DB_USER", "trading_user"),
            "password": os.getenv("DB_PASSWORD", "bitpa$$w0rd"),
            "port": os.getenv("DB_PORT", "5432"),
        }

        # Кэширование
        self.last_tickers_update = None
        self.cached_tickers = {}
        self.last_status_log = 0
        self.status_log_interval = 60

        # ИНИЦИАЛИЗАЦИЯ БД
        self.db_conn = self.init_db()
        if self.db_conn:
            self.log_initial_portfolio()
            self.cleanup_invalid_symbols()
            self.cleanup_dust_positions()
            logger.info("🔄 Первоначальная синхронизация портфеля...")
            self.sync_portfolio_with_exchange()
        else:
            logger.warning(
                "⚠️ База данных не доступна, работаем без сохранения состояния"
            )
        if not self.health_check():
            logger.error("❌ Проверка здоровья не пройдена")
            raise Exception("Health check failed")

    def init_db(self):
        """Инициализация PostgreSQL соединения"""
        try:
            conn = psycopg2.connect(**self.db_config)
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(50) NOT NULL,
                        quantity DECIMAL(20,8) NOT NULL,
                        entry_price DECIMAL(20,8) NOT NULL,
                        current_price DECIMAL(20,8),
                        entry_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        exit_price DECIMAL(20,8),
                        exit_time TIMESTAMP,
                        profit_loss DECIMAL(10,4),
                        status VARCHAR(10) DEFAULT 'active'
                    )
                """)
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_active_symbol
                    ON portfolio (symbol)
                    WHERE status = 'active';
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(50) NOT NULL,
                        side VARCHAR(10) NOT NULL,
                        quantity DECIMAL(20,8) NOT NULL,
                        price DECIMAL(20,8) NOT NULL,
                        fee DECIMAL(20,8),
                        total DECIMAL(20,8) NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            conn.commit()
            logger.info("✅ База данных инициализирована")
            return conn
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            return None

    def calculate_atr(self, symbol, period=14):
        """Расчет Average True Range"""
        try:
            # Проверяем кэш
            cache_key = f"{symbol}_{period}"
            if cache_key in self.atr_cache:
                cache_time, atr_value = self.atr_cache[cache_key]
                if time.time() - cache_time < self.atr_cache_ttl:
                    return atr_value

            ohlcv = self.exchange.fetch_ohlcv(symbol, "1d", limit=period + 1)
            if len(ohlcv) < period + 1:
                return Decimal("0.05")

            true_ranges = []
            for i in range(1, len(ohlcv)):
                high = Decimal(str(ohlcv[i][2]))
                low = Decimal(str(ohlcv[i][3]))
                prev_close = Decimal(str(ohlcv[i - 1][4]))

                tr1 = high - low
                tr2 = abs(high - prev_close)
                tr3 = abs(low - prev_close)

                true_range = max(tr1, tr2, tr3)
                true_ranges.append(float(true_range))

            atr = np.mean(true_ranges) if true_ranges else 0.05
            atr_percentage = Decimal(str(atr / float(ohlcv[-1][4])))

            # Сохраняем в кэш
            self.atr_cache[cache_key] = (time.time(), atr_percentage)
            return atr_percentage

        except Exception as e:
            logger.warning(f"Не удалось рассчитать ATR для {symbol}: {e}")
            return Decimal("0.05")

    def calculate_dynamic_stops(self, symbol, entry_price, atr):
        """Динамические стопы на основе ATR"""
        stop_loss_price = entry_price * (Decimal("1") - self.initial_stop_atr * atr)
        take_profit_price = entry_price * (Decimal("1") + self.take_profit_atr * atr)
        trailing_stop_ratio = Decimal("1") - Decimal("1.5") * atr

        return {
            "stop_loss": stop_loss_price,
            "take_profit": take_profit_price,
            "trailing_stop_ratio": trailing_stop_ratio,
        }

    def calculate_advanced_score(self, ticker_data):
        """Улучшенная система оценки с обработкой ошибок"""
        try:
            symbol = ticker_data["symbol"]
            logger.debug(f"🔍 Расчет score для {symbol}")

            # Расчет факторов с обработкой ошибок
            momentum_score = self.calculate_momentum_score(ticker_data)
            volume_score = self.calculate_volume_score(ticker_data)
            volatility_score = self.calculate_volatility_score(ticker_data)
            structure_score = self.calculate_market_structure_score(ticker_data)

            # Веса факторов
            weights = {
                "momentum": Decimal("0.35"),
                "volume_quality": Decimal("0.25"),
                "volatility_quality": Decimal("0.20"),
                "market_structure": Decimal("0.20"),
            }

            # Итоговый score
            final_score = (
                momentum_score * weights["momentum"]
                + volume_score * weights["volume_quality"]
                + volatility_score * weights["volatility_quality"]
                + structure_score * weights["market_structure"]
            )

            logger.debug(
                f"📊 Score для {symbol}: {final_score:.2f} "
                f"(M:{momentum_score:.1f} V:{volume_score:.1f} "
                f"Vo:{volatility_score:.1f} S:{structure_score:.1f})"
            )

            return final_score

        except Exception as e:
            logger.error(
                f"❌ Ошибка расчета advanced_score для {ticker_data.get('symbol', 'unknown')}: {e}"
            )
            return Decimal("0")

    def calculate_volume_score(self, ticker_data):
        """Упрощенный расчет объема"""
        try:
            volume_usd = ticker_data["volume"]

            if volume_usd > Decimal("1000000"):
                return Decimal("10")
            elif volume_usd > Decimal("500000"):
                return Decimal("8")
            elif volume_usd > Decimal("100000"):
                return Decimal("6")
            elif volume_usd > Decimal("50000"):
                return Decimal("4")
            else:
                return Decimal("2")

        except Exception as e:
            logger.debug(f"Ошибка расчета объема: {e}")
            return Decimal("5")

    def calculate_volatility_score(self, ticker_data):
        """Оценка волатильности"""
        try:
            symbol = ticker_data["symbol"]
            atr = self.calculate_atr(symbol)

            if Decimal("0.03") <= atr <= Decimal("0.08"):
                return Decimal("10")
            elif atr < Decimal("0.03"):
                return Decimal("6")
            else:
                return Decimal("2")

        except Exception as e:
            logger.warning(f"Ошибка оценки волатильности: {e}")
            return Decimal("5")

    def calculate_market_structure_score(self, ticker_data):
        """
        Оценка структуры рынка + EMA Trend Filter.
        """
        try:
            symbol = ticker_data["symbol"]
            # Берем больше свечей для расчета EMA
            ohlcv = self.exchange.fetch_ohlcv(symbol, "4h", limit=60)
            if len(ohlcv) < 50:
                return Decimal("5")  # Недостаточно данных

            closes = [float(x[4]) for x in ohlcv]
            highs = [Decimal(str(x[2])) for x in ohlcv]
            lows = [Decimal(str(x[3])) for x in ohlcv]
            current_price = Decimal(str(ohlcv[-1][4]))

            # --- EMA 50 FILTER ---
            # Простой расчет EMA
            df_closes = pd.Series(closes)
            ema_50 = Decimal(str(df_closes.ewm(span=50, adjust=False).mean().iloc[-1]))

            trend_score = Decimal("0")
            if current_price > ema_50:
                trend_score = Decimal("3")  # Бонус за восходящий тренд
            else:
                trend_score = Decimal("-2")  # Штраф за нисходящий тренд (цена под EMA)

            # --- Price Position Logic ---
            recent_high = max(highs[-20:])
            recent_low = min(lows[-20:])

            if recent_high == recent_low:
                structure_score = Decimal("5")
            else:
                price_position = (current_price - recent_low) / (
                    recent_high - recent_low
                )

                if Decimal("0.3") <= price_position <= Decimal("0.8"):
                    structure_score = Decimal(
                        "7"
                    )  # Оптимально: не на дне и не на самом пике
                elif price_position < Decimal("0.3"):
                    structure_score = Decimal("5")  # Возможно дно, но опасно
                else:
                    structure_score = Decimal("3")  # Слишком дорого

            # Итоговый балл (структура + тренд) ограничим от 0 до 10
            total_score = structure_score + trend_score
            return max(Decimal("0"), min(Decimal("10"), total_score))

        except Exception as e:
            logger.warning(f"Ошибка анализа структуры для {symbol}: {e}")
            return Decimal("5")

    def get_cached_tickers(self):
        """Безопасное получение тикеров с кэшированием"""
        current_time = time.time()

        if (
            self.last_tickers_update is None
            or current_time - self.last_tickers_update > self.tickers_cache_ttl
            or not self.cached_tickers
        ):
            try:
                self.cached_tickers = self.safe_fetch_filtered_tickers()
                self.last_tickers_update = current_time
            except Exception as e:
                logger.error(f"❌ Ошибка обновления кэша тикеров: {e}")

        return self.cached_tickers

    def safe_fetch_filtered_tickers(self):
        """
        ГИБРИДНАЯ ФИЛЬТРАЦИЯ (Smart Lite) с защитой от Rate Limit.
        """
        try:
            tickers = self.exchange.fetch_tickers()
            MIN_VOLUME = Decimal("30000")
            candidates = []

            # 1. Первичная очистка
            for symbol, ticker in tickers.items():
                try:
                    if not symbol.endswith("/USDT"):
                        continue
                    base = symbol.replace("/USDT", "")
                    if base in self.STABLECOINS:
                        continue

                    last = ticker.get("last")
                    vol = ticker.get("quoteVolume")
                    change = ticker.get("percentage", 0)

                    if last is None or vol is None:
                        continue

                    price = Decimal(str(last))
                    volume = Decimal(str(vol))
                    change_pct = Decimal(str(change or 0))

                    if volume < MIN_VOLUME:
                        continue
                    if price <= Decimal("0"):
                        continue

                    candidates.append(
                        {
                            "symbol": symbol,
                            "price": price,
                            "volume": volume,
                            "change_24h": change_pct,
                            "base_symbol": base,
                        }
                    )
                except:
                    continue

            if not candidates:
                return {}

            # 2. Стратегия отбора
            candidates.sort(key=lambda x: x["volume"], reverse=True)
            top_volume = candidates[:30]

            candidates.sort(key=lambda x: x["change_24h"], reverse=True)
            top_gainers = candidates[:20]

            unique_candidates = {
                c["symbol"]: c for c in top_volume + top_gainers
            }.values()

            logger.info(
                f"🏎 Быстрый анализ: отобрано {len(unique_candidates)} монет. Запуск скоринга..."
            )

            filtered = {}

            # 3. Глубокий анализ (запросы к API)
            for cand in unique_candidates:
                try:
                    # !!! ВАЖНО: Пауза чтобы не получить бан API !!!
                    time.sleep(0.2)

                    # Запрашиваем свечи для Momentum Score
                    score = self.calculate_advanced_score(
                        {
                            "symbol": cand["symbol"],
                            "price": cand["price"],
                            "volume": cand["volume"],
                            "change_24h": cand["change_24h"] / 100,
                        }
                    )

                    if score > Decimal("5"):
                        cand["score"] = score
                        # Нормализуем change_24h
                        cand["change_24h"] = cand["change_24h"] / 100
                        filtered[cand["symbol"]] = cand
                        logger.info(f"   ⭐ {cand['symbol']}: Score {score:.1f}")

                except Exception as e:
                    logger.warning(f"Ошибка анализа {cand['symbol']}: {e}")
                    continue

            logger.info(f"✅ Анализ завершен. Кандидатов для покупки: {len(filtered)}")
            return filtered

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки тикеров: {e}")
            return {}

    def enhanced_fetch_filtered_tickers(self):
        """Фильтрация тикеров"""
        try:
            tickers = self.exchange.fetch_tickers()
            filtered = {}
            MIN_24H_VOLUME = Decimal("10000")

            for symbol, ticker in tickers.items():
                try:
                    if not symbol.endswith("/USDT"):
                        continue

                    last_price = ticker.get("last")
                    quote_volume = ticker.get("quoteVolume")
                    if last_price is None or quote_volume is None:
                        continue

                    price = Decimal(str(last_price))
                    volume = Decimal(str(quote_volume))

                    if volume < MIN_24H_VOLUME:
                        continue
                    if price <= Decimal("0") or price > Decimal("100000"):
                        continue

                    base_symbol = symbol.replace("/USDT", "")
                    if base_symbol in self.STABLECOINS:
                        continue

                    enhanced_score = self.calculate_advanced_score(
                        {
                            "price": price,
                            "volume": volume,
                            "change_24h": Decimal(str(ticker.get("percentage", 0))),
                            "symbol": symbol,
                        }
                    )

                    filtered[symbol] = {
                        "price": price,
                        "volume": volume,
                        "change_24h": Decimal(str(ticker.get("percentage", 0))),
                        "symbol": symbol,
                        "base_symbol": base_symbol,
                        "score": enhanced_score,
                    }
                except Exception as e:
                    continue

            return filtered
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки тикеров: {e}")
            return {}

    def get_usdt_balance(self):
        """Надежное получение баланса USDT для Bybit с защитой от отрицательных значений"""
        try:
            balance = self.exchange.fetch_balance(params={"type": "spot"})
            logger.info(f"🔍 Структура баланса: {list(balance.keys())}")

            # 🔴 ЗАЩИТА: Всегда проверяем отрицательные значения
            def safe_positive_decimal(value, default=Decimal("0")):
                """Преобразует значение в Decimal и проверяет что оно не отрицательное"""
                if value is None:
                    return default
                try:
                    str_value = str(value).strip().replace(",", ".")
                    cleaned = "".join(c for c in str_value if c.isdigit() or c in ".-")
                    if cleaned and cleaned != ".":
                        decimal_value = Decimal(cleaned)
                        # 🔴 ВАЖНО: Если значение отрицательное, возвращаем 0
                        return (
                            decimal_value if decimal_value >= Decimal("0") else default
                        )
                    return default
                except:
                    return default

            # 🔴 ПРИОРИТЕТ 1: Свободный баланс (free)
            if "free" in balance and "USDT" in balance["free"]:
                free_balance = balance["free"]["USDT"]
                usdt_balance = safe_positive_decimal(free_balance)
                if usdt_balance > Decimal("0"):
                    logger.info(f"💰 Свободный баланс USDT: {usdt_balance}")
                    return usdt_balance

            # 🔴 ПРИОРИТЕТ 2: Прямой доступ к USDT
            if "USDT" in balance:
                usdt_data = balance["USDT"]
                if isinstance(usdt_data, dict):
                    free_balance = usdt_data.get("free")
                    usdt_balance = safe_positive_decimal(free_balance)
                    if usdt_balance > Decimal("0"):
                        logger.info(f"💰 Баланс USDT (direct): {usdt_balance}")
                        return usdt_balance

            # 🔴 ПРИОРИТЕТ 3: Общий баланс (total) с проверкой на отрицательные значения
            if "total" in balance and "USDT" in balance["total"]:
                total_balance = balance["total"]["USDT"]
                usdt_balance = safe_positive_decimal(total_balance)
                if usdt_balance > Decimal("0"):
                    logger.info(f"💰 Общий баланс USDT: {usdt_balance}")
                    return usdt_balance
                else:
                    logger.warning(
                        f"⚠️ Общий баланс отрицательный или нулевой: {total_balance}"
                    )

            # 🔴 ПРИОРИТЕТ 4: Парсим из 'info' (Bybit specific)
            if "info" in balance and isinstance(balance["info"], dict):
                try:
                    result = balance["info"].get("result", {})
                    if isinstance(result, dict):
                        balances = result.get("balances", [])
                        if not balances and "list" in result:
                            balances = result.get("list", [])

                        for asset in balances:
                            if (
                                asset.get("coin") == "USDT"
                                or asset.get("asset") == "USDT"
                            ):
                                free_balance = (
                                    asset.get("free")
                                    or asset.get("availableToWithdraw")
                                    or asset.get("availableBalance")
                                    or asset.get("walletBalance")
                                )
                                usdt_balance = safe_positive_decimal(free_balance)
                                if usdt_balance > Decimal("0"):
                                    logger.info(
                                        f"💰 Баланс USDT (info): {usdt_balance}"
                                    )
                                    return usdt_balance
                except Exception as e:
                    logger.debug(f"Ошибка парсинга info: {e}")

            # 🔴 ДЕТАЛЬНАЯ ДИАГНОСТИКА
            logger.warning(
                "⚠️ Баланс USDT не найден или отрицательный. Детальная структура:"
            )
            for key, value in balance.items():
                if key in ["info", "timestamp", "datetime"]:
                    continue
                if key in ["free", "used", "total"] and isinstance(value, dict):
                    if "USDT" in value:
                        usdt_value = value["USDT"]
                        logger.warning(
                            f"   {key}.USDT: {usdt_value} (тип: {type(usdt_value)})"
                        )
                elif key == "USDT" and isinstance(value, dict):
                    logger.warning(f"   USDT: {value}")

            logger.error("❌ Не удалось получить положительный баланс USDT")
            return Decimal("0")

        except Exception as e:
            logger.error(f"❌ Критическая ошибка получения баланса: {e}")
            return Decimal("0")

    def get_current_portfolio(self):
        """Получение текущего портфеля"""
        portfolio = {}
        if self.db_conn:
            try:
                with self.db_conn.cursor() as cur:
                    cur.execute("""
                        SELECT symbol, quantity, entry_price, entry_time, current_price
                        FROM portfolio WHERE status = 'active'
                    """)
                    for row in cur.fetchall():
                        symbol, quantity, entry_price, entry_time, current_price = row
                        quantity_dec = Decimal(str(quantity))
                        current_price_dec = (
                            Decimal(str(current_price))
                            if current_price
                            else Decimal("0")
                        )

                        position_value = quantity_dec * current_price_dec
                        if position_value < Decimal("1"):
                            continue

                        portfolio[symbol] = {
                            "quantity": quantity_dec,
                            "entry_price": Decimal(str(entry_price)),
                            "entry_time": entry_time,
                            "current_price": current_price_dec,
                        }
            except Exception as e:
                logger.error(f"❌ Ошибка чтения портфеля: {e}")
        return portfolio

    def sync_portfolio_with_exchange(self):
        """
        Исправленная синхронизация: Принудительно закрывает пыль в БД.
        """
        try:
            logger.info("🔄 ЗАПУСК СИНХРОНИЗАЦИИ ПОРТФЕЛЯ")
            balance = self.exchange.fetch_balance(params={"type": "spot"})
            added_count = 0
            updated_count = 0
            closed_count = 0

            # Вспомогательная функция
            def safe_decimal(value, default=Decimal("0")):
                if value is None:
                    return default
                try:
                    return Decimal(str(value))
                except:
                    return default

            # 1. Получаем список того, что Бот считает активным
            db_active = set()
            if self.db_conn:
                with self.db_conn.cursor() as cur:
                    cur.execute("SELECT symbol FROM portfolio WHERE status = 'active'")
                    db_active = {row[0] for row in cur.fetchall()}

            # 2. Проходим по балансу биржи
            # Нам нужно проверить ВСЕ активные монеты из БД, есть ли они на балансе

            # Сначала соберем реальный баланс в словарь
            real_balances = {}
            for currency, data in balance.items():
                if currency in [
                    "free",
                    "used",
                    "total",
                    "info",
                    "timestamp",
                    "datetime",
                    "USDT",
                ]:
                    continue
                if isinstance(data, dict):
                    free = safe_decimal(data.get("free", 0))
                    total = safe_decimal(data.get("total", 0))
                    # Используем total, так как часть может быть в ордерах
                    if total > Decimal("0"):
                        real_balances[f"{currency}/USDT"] = total

            # А. Обработка того, что есть на бирже
            for symbol, qty in real_balances.items():
                bybit_symbol = symbol.replace("/", "")
                try:
                    ticker = self.exchange.fetch_ticker(bybit_symbol)
                    current_price = safe_decimal(ticker.get("last"))
                except:
                    current_price = Decimal("0")

                val = qty * current_price

                if val > Decimal("2"):
                    # Это реальная позиция -> Обновляем или добавляем
                    with self.db_conn.cursor() as cur:
                        cur.execute(
                            """
                                INSERT INTO portfolio (symbol, quantity, entry_price, current_price, status)
                                VALUES (%s, %s, %s, %s, 'active')
                                ON CONFLICT (symbol) WHERE status = 'active'
                                DO UPDATE SET quantity = EXCLUDED.quantity, current_price = EXCLUDED.current_price
                            """,
                            (
                                symbol,
                                float(qty),
                                float(current_price),
                                float(current_price),
                            ),
                        )
                    if symbol in db_active:
                        updated_count += 1
                    else:
                        added_count += 1
                else:
                    # Это ПЫЛЬ (< $2), но она есть в БД как активная -> ЗАКРЫВАЕМ
                    if symbol in db_active:
                        with self.db_conn.cursor() as cur:
                            cur.execute(
                                "UPDATE portfolio SET status = 'closed' WHERE symbol = %s AND status = 'active'",
                                (symbol,),
                            )
                        logger.info(
                            f"🧹 Закрыта пыль при синхронизации: {symbol} (${val:.2f})"
                        )
                        closed_count += 1

            # Б. Обработка того, что есть в БД, но ИСЧЕЗЛО с биржи (полностью продано)
            for symbol in db_active:
                if symbol not in real_balances:
                    with self.db_conn.cursor() as cur:
                        cur.execute(
                            "UPDATE portfolio SET status = 'closed' WHERE symbol = %s AND status = 'active'",
                            (symbol,),
                        )
                    logger.info(f"👻 Позиция исчезла с баланса (закрываем): {symbol}")
                    closed_count += 1

            self.db_conn.commit()
            logger.info(
                f"📊 Синхронизация: +{added_count} | ~{updated_count} | -{closed_count} (закрыто)"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации: {e}")
            if self.db_conn:
                self.db_conn.rollback()
            return False

    def analyze_portfolio_diversification(self, portfolio, tickers):
        """Анализ диверсификации портфеля"""
        categories_count = {}
        for symbol, position in portfolio.items():
            if symbol in tickers:
                price = tickers[symbol]["price"]
                if price < Decimal("0.01"):
                    category = "micro_cap"
                elif price < Decimal("1"):
                    category = "low_cap"
                elif price < Decimal("10"):
                    category = "mid_cap"
                else:
                    category = "high_cap"
                categories_count[category] = categories_count.get(category, 0) + 1
        return categories_count

    def check_stop_conditions(self, portfolio, tickers):
        """Проверка стоп-условий"""
        positions_to_sell = []
        for symbol, position in portfolio.items():
            if symbol in tickers:
                current_price = tickers[symbol]["price"]
            else:
                try:
                    bybit_symbol = symbol.replace("/", "")
                    ticker = self.exchange.fetch_ticker(bybit_symbol)
                    current_price = Decimal(str(ticker.get("last", 0)))
                except:
                    current_price = position.get("current_price", Decimal("0"))

            entry_price = position["entry_price"]
            entry_time = position.get("entry_time", datetime.now())
            position_value = position["quantity"] * current_price

            if position_value < Decimal("1"):
                continue

            pnl_ratio = current_price / entry_price

            # Стоп-лосс
            if pnl_ratio <= self.stop_loss:
                positions_to_sell.append(
                    (symbol, position, current_price, f"СТОП-ЛОСС ({pnl_ratio:.4f})")
                )
                continue

            # Тейк-профит
            if pnl_ratio >= self.take_profit:
                positions_to_sell.append(
                    (symbol, position, current_price, f"ТЕЙК-ПРОФИТ ({pnl_ratio:.4f})")
                )
                continue

            # Трейлинг-стоп
            if pnl_ratio > Decimal("1.01"):
                if symbol not in self.trailing_stop_max_prices:
                    self.trailing_stop_max_prices[symbol] = current_price
                else:
                    if current_price > self.trailing_stop_max_prices[symbol]:
                        self.trailing_stop_max_prices[symbol] = current_price

                trailing_trigger_price = (
                    self.trailing_stop_max_prices[symbol] * self.trailing_stop
                )
                if current_price <= trailing_trigger_price:
                    positions_to_sell.append(
                        (symbol, position, current_price, f"ТРЕЙЛИНГ-СТОП")
                    )
                    continue

            # Время истекло
            hold_time = datetime.now() - entry_time
            if hold_time > timedelta(hours=self.max_hold_hours):
                positions_to_sell.append(
                    (symbol, position, current_price, f"ВРЕМЯ ИСТЕКЛО ({hold_time})")
                )
                continue

        return positions_to_sell

    def enhanced_rebalance(self, iteration):
        """
        Исправленная ребалансировка: Корректное закрытие сделок в БД.
        """
        try:
            if iteration <= 3:
                logger.info("🔄 Принудительное обновление тикеров...")
                self.cached_tickers = self.safe_fetch_filtered_tickers()
                self.last_tickers_update = time.time()

            if iteration == 1 or iteration % 10 == 0:
                logger.info(f"🔄 Ребалансировка (итерация #{iteration})")

            self.auto_adjust_parameters()

            # 1. Синхронизация (теперь она умеет убивать зомби-позиции)
            logger.info("🔄 Синхронизация портфеля...")
            self.sync_portfolio_with_exchange()

            available_balance = self.get_usdt_balance()
            tickers = self.get_cached_tickers()
            current_portfolio = self.get_current_portfolio()

            # ЛОГИРОВАНИЕ
            logger.info("📊 ТЕКУЩИЙ СТАТУС:")
            logger.info(f"   💰 Баланс: {available_balance:.2f} USDT")
            logger.info(f"   💸 Резерв: {self.reserve_cash} USDT")
            # Считаем только реальные позиции для логирования
            real_pos = [
                k
                for k, v in current_portfolio.items()
                if (v["quantity"] * v["current_price"]) > Decimal("2")
            ]
            logger.info(
                f"   📦 Позиций: {len(real_pos)}/{self.max_positions} (Всего в БД: {len(current_portfolio)})"
            )

            # --- ПРОДАЖА ---
            positions_to_sell = self.check_stop_conditions(current_portfolio, tickers)

            if positions_to_sell:
                logger.info("🚨 АКТИВНЫЕ СТОП-УСЛОВИЯ:")

                for symbol, position, current_price, reason in positions_to_sell:
                    logger.info(f"   🔻 {symbol}: {reason} | Цена: {current_price:.6f}")

                    # Попытка продажи (вернет True, если продал ИЛИ если это пыль)
                    success = self.smart_order_manager.execute_smart_sell(
                        symbol, position["quantity"], current_price
                    )

                    if success:
                        # !!! ВАЖНЕЙШЕЕ ИСПРАВЛЕНИЕ !!!
                        # Сразу закрываем позицию в БД, не дожидаясь синхронизации
                        with self.db_conn.cursor() as cur:
                            cur.execute(
                                "UPDATE portfolio SET status = 'closed' WHERE symbol = %s",
                                (symbol,),
                            )
                        self.db_conn.commit()

                        # Статистика
                        pnl = (current_price - position["entry_price"]) * position[
                            "quantity"
                        ]
                        pnl_pct = (current_price / position["entry_price"]) - Decimal(
                            "1"
                        )

                        self.performance_analytics.add_trade(
                            {
                                "symbol": symbol,
                                "side": "sell",
                                "quantity": position["quantity"],
                                "entry_price": position["entry_price"],
                                "exit_price": current_price,
                                "pnl": pnl,
                                "pnl_pct": pnl_pct,
                                "commission": 0,
                            }
                        )
                        self.kelly_manager.update_trade_history(
                            {"pnl": pnl, "pnl_pct": pnl_pct}
                        )

                        logger.info(f"   ✅ Продано (и закрыто в БД): {symbol}")
                    else:
                        logger.error(f"   ❌ Ошибка продажи: {symbol}")

                # Обновляем данные после продаж
                current_portfolio = self.get_current_portfolio()
                available_balance = self.get_usdt_balance()

            # --- ПОКУПКА ---
            # Пересчитываем свободные слоты на основе РЕАЛЬНЫХ позиций (> $2)
            real_positions_count = 0
            for sym, pos in current_portfolio.items():
                if (pos["quantity"] * pos["current_price"]) > Decimal("2"):
                    real_positions_count += 1

            # Принудительно обнуляем счетчик, если бот застрял в режиме снайпера, но позиций нет
            if real_positions_count == 0 and len(current_portfolio) > 0:
                logger.info(
                    "🧹 Обнаружены фантомные позиции, игнорируем их для покупки."
                )
                real_positions_count = 0

            has_free_slots = real_positions_count < self.max_positions
            available_for_trading = available_balance - self.reserve_cash

            # Размер позиции (защита от None)
            target_size = self.min_position_size
            if target_size is None:
                target_size = Decimal("10")

            can_trade = available_for_trading >= target_size

            if can_trade and has_free_slots:
                logger.info("🎯 ПОИСК ТОРГОВЫХ ВОЗМОЖНОСТЕЙ...")
                best_opportunities = self.find_optimized_opportunities(
                    tickers, current_portfolio
                )

                # Лимит покупок за цикл
                bought_count = 0

                for symbol, score, price, category in best_opportunities:
                    if bought_count >= 1:
                        break  # Покупаем по 1 за раз для безопасности

                    if score < Decimal("6"):
                        continue

                    # Проверка денег (еще раз)
                    if available_for_trading < target_size:
                        break

                    # Определяем размер позиции (Sniper mode = all in, иначе стандарт)
                    buy_amount = target_size
                    if self.max_positions == 1:
                        buy_amount = available_for_trading  # На все деньги

                    logger.info(f"🛒 ПОПЫТКА ПОКУПКИ {symbol} на {buy_amount:.2f} USDT")

                    success = self.smart_order_manager.execute_smart_buy(
                        symbol, buy_amount
                    )

                    if success:
                        bought_count += 1
                        available_for_trading -= buy_amount
                        # Запись в БД сделает следующая синхронизация или можно добавить тут
                        logger.info(f"✅ УСПЕШНАЯ ПОКУПКА: {symbol}")
                    else:
                        logger.error(f"❌ ОШИБКА ПОКУПКИ: {symbol}")
            else:
                if not can_trade:
                    logger.info(
                        f"💤 Ждем средств ({available_for_trading:.2f} < {target_size:.2f})"
                    )
                if not has_free_slots:
                    logger.info(
                        f"📦 Нет слотов ({real_positions_count}/{self.max_positions})"
                    )

            # Отчеты и очистка
            if iteration % 288 == 0:
                self.performance_analytics.generate_performance_report()
            self.cleanup_old_cache()

            return True

        except Exception as e:
            logger.error(f"❌ ОШИБКА РЕБАЛАНСИРОВКИ: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return False

    def calculate_momentum_score(self, ticker_data):
        """Исправленный расчет моментума (Multi-timeframe)"""
        try:
            symbol = ticker_data["symbol"]
            # Используем более значимые таймфреймы для тренда
            timeframes = ["15m", "1h", "4h"]
            momentum_scores = []

            for tf in timeframes:
                try:
                    # Берем 25 свечей, чтобы гарантированно получить [-21]
                    ohlcv = self.exchange.fetch_ohlcv(symbol, tf, limit=25)
                    if len(ohlcv) < 22:
                        continue

                    current_price = Decimal(str(ohlcv[-1][4]))

                    # ИСПРАВЛЕНО: Корректные индексы для сравнения
                    # [-6] = 5 свечей назад
                    # [-11] = 10 свечей назад
                    # [-21] = 20 свечей назад
                    price_5 = Decimal(str(ohlcv[-6][4]))
                    price_10 = Decimal(str(ohlcv[-11][4]))
                    price_20 = Decimal(str(ohlcv[-21][4]))

                    if price_5 == 0 or price_10 == 0 or price_20 == 0:
                        continue

                    # Рост в процентах
                    mom_5 = (current_price - price_5) / price_5
                    mom_10 = (current_price - price_10) / price_10
                    mom_20 = (current_price - price_20) / price_20

                    # Взвешенное среднее: свежий импульс важнее
                    tf_momentum = (
                        mom_5 * Decimal("0.5")
                        + mom_10 * Decimal("0.3")
                        + mom_20 * Decimal("0.2")
                    )
                    momentum_scores.append(tf_momentum)

                except Exception as e:
                    continue

            if momentum_scores:
                avg_momentum = sum(momentum_scores) / len(momentum_scores)
                # Нормализация: увеличиваем чувствительность (умножаем на 2000)
                # Значение 0.005 (0.5% роста) даст score 10
                score = avg_momentum * Decimal("2000")
                normalized_score = max(Decimal("0"), min(Decimal("10"), score))
                return normalized_score
            else:
                return Decimal("0")  # Если данных нет, скор 0 (не торгуем)

        except Exception as e:
            logger.warning(
                f"❌ Ошибка расчета моментума для {ticker_data.get('symbol')}: {e}"
            )
            return Decimal("0")

    def auto_adjust_parameters(self):
        """Адаптация под реальный депозит (исправленная логика)."""
        try:
            real_balance = self.get_usdt_balance()

            # Получаем текущее количество активных позиций
            active_positions = len(self.get_current_portfolio())

            logger.info(
                f"💰 Реальный баланс: {real_balance:.2f} | Позиций: {active_positions}"
            )

            if real_balance < Decimal("20"):
                # === РЕЖИМ МИКРО-ДЕПОЗИТА (< $20) ===
                # Стратегия: Снайпер (одна точная сделка на весь объем)
                # Причина: Если разбить $15 на 3 части по $5, комиссии и проскальзывания съедят прибыль.

                self.max_positions = 1
                self.reserve_cash = Decimal("1")  # $1 на всякий случай

                available = real_balance - self.reserve_cash

                # Если у нас уже есть позиция, новые не открываем
                if active_positions >= 1:
                    self.min_position_size = Decimal("999999")  # Блокируем покупку
                    self.max_position_size = Decimal("999999")
                else:
                    # Если позиций нет, заходим "на всю котлету" (но не меньше $5.5)
                    trade_amount = max(Decimal("5.5"), available)
                    self.min_position_size = trade_amount
                    self.max_position_size = trade_amount * Decimal(
                        "1.1"
                    )  # чуть больше для гибкости

                logger.info("⚠️ РЕЖИМ <$20: Макс 1 позиция (Sniper Mode)")

            elif real_balance < Decimal("50"):
                # === РЕЖИМ МАЛОГО ДЕПОЗИТА ($20 - $50) ===
                self.max_positions = 2
                self.reserve_cash = Decimal("2")

                share = (real_balance - self.reserve_cash) / 2
                self.min_position_size = max(Decimal("6"), share * Decimal("0.9"))
                self.max_position_size = max(Decimal("6"), share * Decimal("1.1"))

                logger.info("⚠️ РЕЖИМ $20-$50: Макс 2 позиции")

            else:
                # === СТАНДАРТНЫЙ РЕЖИМ (> $50) ===
                self.max_positions = 3  # Или больше, если баланс растет
                self.reserve_cash = Decimal("5")

                share = (real_balance - self.reserve_cash) / Decimal("3")
                self.min_position_size = max(Decimal("10"), share * Decimal("0.8"))
                self.max_position_size = max(Decimal("12"), share * Decimal("1.2"))

            # Синхронизируем с менеджером капитала
            self.kelly_manager.total_capital = real_balance

        except Exception as e:
            logger.error(f"Ошибка автонастройки: {e}")

    def find_optimized_opportunities(self, tickers, portfolio):
        """Поиск оптимизированных торговых возможностей"""
        opportunities = []

        # АНАЛИЗ ДИВЕРСИФИКАЦИИ
        current_categories = self.analyze_portfolio_diversification(portfolio, tickers)

        logger.info("🔍 АНАЛИЗ РЫНОЧНЫХ ВОЗМОЖНОСТЕЙ...")

        for symbol, data in tickers.items():
            if symbol not in portfolio:
                score = data.get("score", Decimal("0"))
                price = data["price"]

                # 🔴 ДОПОЛНИТЕЛЬНЫЕ ПРОВЕРКИ
                # Проверка ликвидности
                if data["volume"] < Decimal("50000"):  # Минимум $50k объема
                    continue

                # Проверка волатильности через ATR
                atr = self.calculate_atr(symbol)
                if atr > Decimal("0.15"):  # Слишком высокая волатильность
                    continue

                # Определение категории актива
                category = "unknown"
                if price < Decimal("0.01"):
                    category = "micro_cap"
                elif price < Decimal("1"):
                    category = "low_cap"
                elif price < Decimal("10"):
                    category = "mid_cap"
                else:
                    category = "high_cap"

                # 🔴 БОНУС ЗА ДИВЕРСИФИКАЦИЮ
                diversification_bonus = Decimal("0")
                if current_categories.get(category, 0) == 0:
                    diversification_bonus = Decimal("3")  # Новая категория
                elif current_categories.get(category, 0) <= 1:
                    diversification_bonus = Decimal("1")  # Мало позиций в категории

                final_score = score + diversification_bonus

                opportunities.append((symbol, final_score, price, category))

        # СОРТИРОВКА ПО SCORE
        opportunities.sort(key=lambda x: x[1], reverse=True)

        logger.info(f"   Найдено возможностей: {len(opportunities)}")

        return opportunities[:10]  # Возвращаем топ-10

    def cleanup_old_cache(self):
        """Очистка устаревших данных кэша"""
        current_time = time.time()

        # Очистка ATR кэша
        expired_keys = []
        for key, (timestamp, value) in self.atr_cache.items():
            if current_time - timestamp > self.atr_cache_ttl:
                expired_keys.append(key)

        for key in expired_keys:
            del self.atr_cache[key]

        if expired_keys:
            logger.debug(f"🧹 Очищено {len(expired_keys)} устаревших ATR записей")

    def log_enhanced_portfolio_status(self, portfolio, tickers):
        """Улучшенное логирование статуса портфеля"""
        try:
            total_value = Decimal("0")
            total_pnl = Decimal("0")
            category_value = {}

            logger.info("📊 ДЕТАЛЬНЫЙ СТАТУС ПОРТФЕЛЯ:")

            if not portfolio:
                logger.info("   💡 Портфель пуст")
                return

            for symbol, position in portfolio.items():
                # Получаем актуальную цену
                current_price = position.get("current_price", Decimal("0"))
                if symbol in tickers:
                    current_price = tickers[symbol]["price"]

                quantity = position["quantity"]
                entry_price = position["entry_price"]

                current_value = quantity * current_price
                total_value += current_value

                # Расчет PnL
                pnl = (current_price - entry_price) * quantity
                total_pnl += pnl
                pnl_percent = ((current_price / entry_price) - Decimal("1")) * Decimal(
                    "100"
                )

                # Категоризация
                category = "unknown"
                if current_price < Decimal("0.01"):
                    category = "micro_cap"
                elif current_price < Decimal("1"):
                    category = "low_cap"
                elif current_price < Decimal("10"):
                    category = "mid_cap"
                else:
                    category = "high_cap"

                category_value[category] = (
                    category_value.get(category, Decimal("0")) + current_value
                )

                # Цветовая индикация PnL
                pnl_sign = "🟢" if pnl >= 0 else "🔴"
                logger.info(
                    f"   {symbol} [{category}]: {pnl_sign} PnL: {pnl:.4f} USDT ({pnl_percent:.2f}%)"
                )

            # БАЛАНС И ОБЩАЯ СТАТИСТИКА
            balance = self.get_usdt_balance()
            total_assets = total_value + balance

            logger.info(f"💰 РАСПРЕДЕЛЕНИЕ АКТИВОВ:")
            logger.info(f"   Баланс USDT: {balance:.2f} USDT")
            logger.info(f"   Стоимость позиций: {total_value:.2f} USDT")
            logger.info(f"   Общие активы: {total_assets:.2f} USDT")

            logger.info(f"🎯 РАСПРЕДЕЛЕНИЕ ПО КАТЕГОРИЯМ:")
            for category, value in category_value.items():
                percent = (
                    (value / total_value * Decimal("100"))
                    if total_value > Decimal("0")
                    else Decimal("0")
                )
                logger.info(f"   {category}: {value:.2f} USDT ({percent:.1f}%)")

            total_pnl_sign = "🟢" if total_pnl >= 0 else "🔴"
            logger.info(f"📈 ОБЩИЙ PnL: {total_pnl_sign} {total_pnl:.4f} USDT")

        except Exception as e:
            logger.error(f"❌ Ошибка логирования портфеля: {e}")

    def run_optimized(self):
        """Основной цикл с улучшенной обработкой ошибок"""
        logger.info("🚀 Запуск улучшенного спот-бота Bybit")

        # --- ИСПРАВЛЕННЫЙ БЛОК ПРОВЕРКИ ---
        try:
            balance = self.get_usdt_balance()
            portfolio = self.get_current_portfolio()

            if balance <= Decimal("0"):
                if len(portfolio) > 0:
                    logger.warning(
                        "⚠️ Баланс USDT равен 0, но найдены активные позиции. Бот переходит в режим управления позициями."
                    )
                else:
                    logger.error(
                        "❌ Баланс USDT равен 0 и портфель пуст. Пополните депозит."
                    )
                    return
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            return
        # ----------------------------------

        iteration = 0
        consecutive_errors = 0
        max_consecutive_errors = 3

        while True:
            try:
                iteration += 1
                logger.info(f"🔄 Итерация #{iteration}")

                if not self.db_conn or self.db_conn.closed:
                    self.db_conn = self.init_db()

                success = self.enhanced_rebalance(iteration)

                if success:
                    consecutive_errors = 0
                    time.sleep(self.rebalance_interval)
                else:
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error("🚨 Слишком много ошибок подряд, увеличение паузы")
                        time.sleep(300)  # 5 минут паузы
                        consecutive_errors = 0
                    else:
                        time.sleep(60)  # 1 минута паузы при ошибке

            except KeyboardInterrupt:
                logger.info("\n⏹️ Остановка бота по запросу пользователя...")
                break
            except Exception as e:
                logger.error(f"❌ Критическая ошибка в основном цикле: {e}")
                consecutive_errors += 1
                time.sleep(60)

    def health_check(self):
        """Проверка здоровья подключения к бирже и БД"""
        try:
            # Проверка биржи
            balance = self.get_usdt_balance()
            if balance == Decimal("0"):
                logger.warning(
                    "⚠️ Баланс USDT равен 0. Проверьте депозит или API ключи."
                )
            else:
                logger.info(f"✅ Подключение к бирже: OK (баланс: {balance} USDT)")

            # Проверка БД
            if self.db_conn:
                with self.db_conn.cursor() as cur:
                    cur.execute("SELECT 1")
                logger.info("✅ Подключение к БД: OK")
            else:
                logger.warning("⚠️ Подключение к БД: отсутствует")

            return True  # Возвращаем True даже при нулевом балансе
        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return False

    # Вспомогательные методы (сокращенно)
    def log_initial_portfolio(self):
        """Логирование начального портфеля"""
        try:
            portfolio = self.get_current_portfolio()
            balance = self.get_usdt_balance()
            logger.info("📊 НАЧАЛЬНОЕ СОСТОЯНИЕ ПОРТФЕЛЯ:")
            logger.info(f"💰 Баланс USDT: {balance}")
            logger.info(f"📦 Позиций в портфеле: {len(portfolio)}")
        except Exception as e:
            logger.error(f"❌ Ошибка логирования портфеля: {e}")

    def cleanup_invalid_symbols(self):
        """Очистка невалидных символов"""
        try:
            if not self.db_conn:
                return
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol FROM portfolio
                    WHERE status = 'active' AND (symbol LIKE '%:%' OR symbol NOT LIKE '%/%')
                """)
                invalid_symbols = [row[0] for row in cur.fetchall()]
                for symbol in invalid_symbols:
                    cur.execute(
                        "UPDATE portfolio SET status = 'closed' WHERE symbol = %s",
                        (symbol,),
                    )
                self.db_conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка очистки символов: {e}")

    def cleanup_dust_positions(self):
        """
        Агрессивная очистка пылевых позиций из БД.
        Удаляет все записи, стоимость которых меньше $2 (минимум биржи ~5$, но пыль это <1-2$).
        """
        try:
            if not self.db_conn:
                return

            logger.info("🧹 Запуск очистки пыли и мусорных позиций...")

            # 1. Сначала пометим закрытыми те, где quantity * price < 2 USDT
            with self.db_conn.cursor() as cur:
                # Получаем список активных, чтобы проверить цену
                cur.execute(
                    "SELECT symbol, quantity, current_price FROM portfolio WHERE status = 'active'"
                )
                rows = cur.fetchall()

                dust_symbols = []
                for row in rows:
                    symbol, quantity, price = row
                    # Если цена в БД 0, попробуем получить текущую (если есть в кэше) или пропустим
                    val = float(quantity) * float(price if price else 0)

                    # Если стоимость позиции меньше 2$, считаем это мусором, который нельзя продать
                    if val < 2.0:
                        dust_symbols.append(symbol)
                        logger.info(
                            f"   🗑 Обнаружена пыль: {symbol} (${val:.4f}) -> Удаляем из активных"
                        )

                # Массовое обновление статуса
                for sym in dust_symbols:
                    cur.execute(
                        "UPDATE portfolio SET status = 'closed' WHERE symbol = %s",
                        (sym,),
                    )

            self.db_conn.commit()
            logger.info(f"✅ Очистка завершена. Удалено позиций: {len(dust_symbols)}")

        except Exception as e:
            logger.error(f"❌ Ошибка очистки пыли: {e}")
            self.db_conn.rollback()


if __name__ == "__main__":
    required_env_vars = ["BYBIT_API_KEY", "BYBIT_API_SECRET"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        exit(1)

    bot = BybitSpotBot()

    try:
        bot.run_optimized()
    except KeyboardInterrupt:
        logger.info("\n⏹️ Остановка бота...")
    finally:
        if bot.db_conn:
            bot.db_conn.close()
