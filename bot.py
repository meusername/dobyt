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
    handlers=[
        logging.FileHandler("dobyt.log", encoding="utf-8"),  # Пишет в файл
        logging.StreamHandler(),  # Пишет в консоль
    ],
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
        Быстрая покупка. ИСПРАВЛЕНО: Уменьшен буфер цены до 0.15% для Bybit.
        """
        try:
            orderbook = self.exchange.fetch_order_book(symbol, limit=5)
            best_ask = float(orderbook["asks"][0][0])

            # ИСПРАВЛЕНО: 0.15% вместо 0.5%, чтобы не ловить ошибку 'Price too high'
            buy_price = best_ask * 1.0015

            # Буфер USDT на комиссию
            usable_usdt = float(amount_usdt) * 0.99
            raw_quantity = usable_usdt / buy_price

            symbol_precision = self.exchange.market(symbol)

            # Проверки лимитов
            min_cost = symbol_precision["limits"]["cost"]["min"]
            min_amount = symbol_precision["limits"]["amount"]["min"]

            if min_cost and usable_usdt < min_cost:
                return False
            if min_amount and raw_quantity < min_amount:
                return False

            price_final = self.exchange.price_to_precision(symbol, buy_price)
            amount_final = self.exchange.amount_to_precision(symbol, raw_quantity)

            logger.info(f"🛒 Покупка {symbol}: {amount_final} @ {price_final}")

            order = self.exchange.create_order(
                symbol=symbol,
                type="limit",
                side="buy",
                amount=amount_final,
                price=price_final,
            )

            return self.monitor_order_execution(order["id"], symbol, timeout=5)

        except Exception as e:
            logger.error(f"❌ Ошибка Smart Buy для {symbol}: {e}")
            return False

    def execute_smart_sell(
        self, symbol, quantity, current_price=None, max_slippage=Decimal("0.005")
    ):
        """
        Умная продажа. ИСПРАВЛЕНО: Уменьшен буфер цены до 0.15%.
        """
        try:
            base_currency = symbol.split("/")[0]
            try:
                balance = self.exchange.fetch_balance()
            except:
                return False

            available = 0
            if "free" in balance and base_currency in balance["free"]:
                available = float(balance["free"][base_currency])

            if available <= 0:
                logger.warning(f"⚠️ Баланс {symbol} = 0. Удаляем из БД.")
                return True

            if current_price is None:
                ticker = self.exchange.fetch_ticker(symbol)
                current_price = float(ticker["last"])

            # Проверка на пыль
            if available * float(current_price) < 2.0:
                logger.warning(f"🧹 Пыль {symbol} < $2. Удаляем.")
                return True

            amount_final = self.exchange.amount_to_precision(symbol, available)
            if float(amount_final) > available:
                amount_final = self.exchange.amount_to_precision(
                    symbol, available * 0.999
                )

            # ИСПРАВЛЕНО: Цена продажи чуть ниже рынка (0.15%), но в пределах лимитов Bybit
            orderbook = self.exchange.fetch_order_book(symbol, limit=5)
            best_bid = float(orderbook["bids"][0][0])
            sell_price = best_bid * 0.9985
            price_final = self.exchange.price_to_precision(symbol, sell_price)

            logger.info(f"🔻 Продажа {symbol}: {amount_final} @ {price_final}")

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
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                try:
                    order = self.exchange.fetch_order(order_id, symbol)
                except Exception as e:
                    if "does not exist" in str(e) or "not found" in str(e):
                        pass
                    # Проверяем открытые ордера
                    open_orders = self.exchange.fetch_open_orders(symbol)
                    if not any(o["id"] == str(order_id) for o in open_orders):
                        if time.time() - start_time > 2:
                            return True
                    time.sleep(1)
                    continue

                if order["status"] == "closed":
                    return True
                elif order["status"] == "canceled":
                    return False

                time.sleep(0.5)
            except:
                time.sleep(1)

        try:
            self.exchange.cancel_order(order_id, symbol)
            return False
        except:
            return True  # Скорее всего уже исполнился


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


'''
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
        self.stop_loss = Decimal("0.94")
        self.take_profit = Decimal("1.08")
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
        ИСПРАВЛЕНО: Увеличен лимит свечей для корректного расчета EMA.
        """
        try:
            symbol = ticker_data["symbol"]
            # ВАЖНО: EMA требует "разгона", берем 200 свечей вместо 60
            ohlcv = self.exchange.fetch_ohlcv(symbol, "4h", limit=200)
            if len(ohlcv) < 150:
                return Decimal("5")  # Недостаточно данных

            closes = [float(x[4]) for x in ohlcv]
            highs = [Decimal(str(x[2])) for x in ohlcv]
            lows = [Decimal(str(x[3])) for x in ohlcv]
            current_price = Decimal(str(ohlcv[-1][4]))

            # --- EMA 50 FILTER ---
            df_closes = pd.Series(closes)
            # Используем min_periods, чтобы не получать NaN в начале
            ema_50_val = (
                df_closes.ewm(span=50, adjust=False, min_periods=50).mean().iloc[-1]
            )
            ema_50 = Decimal(str(ema_50_val))

            trend_score = Decimal("0")
            if current_price > ema_50:
                trend_score = Decimal("3")  # Бонус за восходящий тренд
            else:
                trend_score = Decimal("-2")  # Штраф за нисходящий тренд

            # --- Price Position Logic ---
            # Смотрим, где цена относительно последних 20 свечей
            recent_high = max(highs[-20:])
            recent_low = min(lows[-20:])

            if recent_high == recent_low:
                structure_score = Decimal("5")
            else:
                price_position = (current_price - recent_low) / (
                    recent_high - recent_low
                )

                if Decimal("0.3") <= price_position <= Decimal("0.8"):
                    structure_score = Decimal("7")  # Оптимально
                elif price_position < Decimal("0.3"):
                    structure_score = Decimal("5")  # Дешево, но может падать дальше
                else:
                    structure_score = Decimal("3")  # Дороговато

            # Итоговый балл (0-10)
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

    def enhanced_rebalance(self, iteration):
        """
        Исправленная ребалансировка: RSI + Cooldown + Исправленная запись в БД.
        """
        try:
            if iteration <= 3:
                logger.info("🔄 Принудительное обновление тикеров...")
                self.cached_tickers = self.safe_fetch_filtered_tickers()
                self.last_tickers_update = time.time()

            if iteration == 1 or iteration % 10 == 0:
                logger.info(f"🔄 Ребалансировка (итерация #{iteration})")

            self.auto_adjust_parameters()

            logger.info("🔄 Синхронизация портфеля...")
            self.sync_portfolio_with_exchange()

            available_balance = self.get_usdt_balance()
            tickers = self.get_cached_tickers()
            current_portfolio = self.get_current_portfolio()

            # Логируем только реальные позиции > $2
            real_pos = []
            for k, v in current_portfolio.items():
                val = v["quantity"] * v["current_price"]
                if val > Decimal("2"):
                    real_pos.append(k)

            logger.info("📊 ТЕКУЩИЙ СТАТУС:")
            logger.info(f"   💰 Баланс: {available_balance:.2f} USDT")
            logger.info(f"   📦 Позиций: {len(real_pos)}/{self.max_positions}")

            # === ДОБАВЛЕНО: ВЫВОД ТЕКУЩИХ ПОЗИЦИЙ ===
            if real_pos:
                logger.info("   💎 УДЕРЖИВАЕМЫЕ АКТИВЫ:")
                for symbol in real_pos:
                    pos = current_portfolio[symbol]
                    q = pos["quantity"]
                    ep = pos["entry_price"]
                    cp = pos["current_price"]

                    # Считаем PnL
                    pnl_val = (cp - ep) * q
                    pnl_pct = ((cp / ep) - 1) * 100

                    icon = "🟢" if pnl_val >= 0 else "🔴"
                    logger.info(
                        f"      {icon} {symbol}: {pnl_pct:.2f}% (${pnl_val:.4f}) | Цена: {cp}"
                    )

            # --- ПРОДАЖА ---
            positions_to_sell = self.check_stop_conditions(current_portfolio, tickers)

            if positions_to_sell:
                logger.info("🚨 АКТИВНЫЕ СТОП-УСЛОВИЯ:")
                for symbol, position, current_price, reason in positions_to_sell:
                    logger.info(f"   🔻 {symbol}: {reason} | Цена: {current_price:.6f}")

                    success = self.smart_order_manager.execute_smart_sell(
                        symbol, position["quantity"], current_price
                    )

                    if success:
                        # Рассчитываем PnL для записи
                        quantity = Decimal(str(position["quantity"]))
                        entry_price = Decimal(str(position["entry_price"]))
                        curr_price_dec = Decimal(str(current_price))

                        pnl = (curr_price_dec - entry_price) * quantity
                        pnl_pct = (curr_price_dec / entry_price) - Decimal("1")

                        # !!! ВАЖНО: Обновляем БД с прибылью и временем !!!
                        with self.db_conn.cursor() as cur:
                            cur.execute(
                                """
                                    UPDATE portfolio
                                    SET status = 'closed',
                                        exit_price = %s,
                                        exit_time = NOW(),
                                        profit_loss = %s
                                    WHERE symbol = %s AND status = 'active'
                                """,
                                (float(curr_price_dec), float(pnl), symbol),
                            )
                        self.db_conn.commit()

                        # Аналитика
                        self.performance_analytics.add_trade(
                            {
                                "symbol": symbol,
                                "side": "sell",
                                "quantity": quantity,
                                "entry_price": entry_price,
                                "exit_price": curr_price_dec,
                                "pnl": pnl,
                                "pnl_pct": pnl_pct,
                                "commission": 0,
                            }
                        )
                        self.kelly_manager.update_trade_history(
                            {"pnl": pnl, "pnl_pct": pnl_pct}
                        )

                        logger.info(
                            f"   ✅ Продано и сохранено: {symbol} | PnL: {pnl:.4f} USDT"
                        )
                    else:
                        logger.error(f"   ❌ Ошибка продажи: {symbol}")

                available_balance = self.get_usdt_balance()
                current_portfolio = self.get_current_portfolio()

            # --- ПОКУПКА ---
            real_positions_count = 0
            for sym, pos in current_portfolio.items():
                if (pos["quantity"] * pos["current_price"]) > Decimal("2"):
                    real_positions_count += 1

            has_free_slots = real_positions_count < self.max_positions
            available_for_trading = available_balance - self.reserve_cash

            target_size = self.min_position_size
            if target_size is None:
                target_size = Decimal("10")

            can_trade = available_for_trading >= target_size

            if can_trade and has_free_slots:
                logger.info("🎯 ПОИСК ТОРГОВЫХ ВОЗМОЖНОСТЕЙ...")
                # Здесь теперь работает Cooldown
                best_opportunities = self.find_optimized_opportunities(
                    tickers, current_portfolio
                )

                bought_count = 0
                max_buys_per_cycle = 3

                for symbol, score, price, category in best_opportunities:
                    if bought_count >= max_buys_per_cycle:
                        break
                    if self.max_positions == 1 and bought_count >= 1:
                        break

                    if score < Decimal("6"):
                        continue

                    # === RSI ФИЛЬТР ===
                    rsi = self.calculate_rsi(symbol)
                    if rsi > Decimal("75"):
                        logger.info(f"   ⚠️ Пропуск {symbol}: RSI перегрет ({rsi:.1f})")
                        continue

                    if available_for_trading < target_size:
                        break

                    buy_amount = target_size
                    if self.max_positions == 1:
                        buy_amount = available_for_trading

                    logger.info(
                        f"🛒 ПОПЫТКА ПОКУПКИ {symbol} на {buy_amount:.2f} USDT (RSI: {rsi:.1f})"
                    )

                    success = self.smart_order_manager.execute_smart_buy(
                        symbol, buy_amount
                    )

                    if success:
                        bought_count += 1
                        available_for_trading -= buy_amount
                        logger.info(f"✅ УСПЕШНАЯ ПОКУПКА: {symbol}")
                        if self.max_positions == 1:
                            break
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

    def calculate_rsi(self, symbol, period=14):
        """Расчет RSI для фильтрации перекупленности."""
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, "15m", limit=100)
            if len(ohlcv) < period + 1:
                return Decimal("50")

            closes = [float(x[4]) for x in ohlcv]
            df = pd.Series(closes)
            delta = df.diff()

            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

            rs = gain / loss.replace(0, 0.0000001)
            rsi = 100 - (100 / (1 + rs))
            return Decimal(str(rsi.iloc[-1]))
        except:
            return Decimal("50")

    def check_stop_conditions(self, portfolio, tickers):
        """
        Проверка стоп-условий.
        ИСПРАВЛЕНО: Расширены стопы (6%/8%) и исправлен баг с Timezone.
        """
        positions_to_sell = []
        for symbol, position in portfolio.items():
            if symbol in tickers:
                current_price = tickers[symbol]["price"]
            else:
                try:
                    ticker = self.exchange.fetch_ticker(symbol.replace("/", ""))
                    current_price = Decimal(str(ticker.get("last", 0)))
                except:
                    current_price = position.get("current_price", Decimal("0"))

            entry_price = position["entry_price"]

            # --- FIX TIMEZONE ---
            # Считаем время удержания надежно (игнорируя баги таймзон)
            entry_time = position.get("entry_time")
            now = datetime.now()

            if not entry_time:
                entry_time = now

            # Если разница во времени выглядит странно (отрицательная или > 24 часов для новой сделки),
            # считаем, что прошло 0 времени.
            delta = now - entry_time
            if delta.days < 0 or delta.days > 100:
                hold_time = timedelta(seconds=0)
            else:
                hold_time = delta

            position_value = position["quantity"] * current_price
            if position_value < Decimal("1"):
                continue

            pnl_ratio = current_price / entry_price

            # 1. Стоп-лосс (расширен до -6% для защиты от шума)
            if pnl_ratio <= Decimal("0.94"):
                positions_to_sell.append(
                    (symbol, position, current_price, f"СТОП-ЛОСС ({pnl_ratio:.4f})")
                )
                continue

            # 2. Тейк-профит (расширен до +8%)
            if pnl_ratio >= Decimal("1.08"):
                positions_to_sell.append(
                    (symbol, position, current_price, f"ТЕЙК-ПРОФИТ ({pnl_ratio:.4f})")
                )
                continue

            # 3. Трейлинг (включаем только если есть прибыль +3%)
            if pnl_ratio > Decimal("1.03"):
                if symbol not in self.trailing_stop_max_prices:
                    self.trailing_stop_max_prices[symbol] = current_price
                else:
                    if current_price > self.trailing_stop_max_prices[symbol]:
                        self.trailing_stop_max_prices[symbol] = current_price

                # Откат 2% от максимума
                trailing_trigger = self.trailing_stop_max_prices[symbol] * Decimal(
                    "0.98"
                )
                if current_price <= trailing_trigger:
                    positions_to_sell.append(
                        (symbol, position, current_price, f"ТРЕЙЛИНГ-СТОП")
                    )
                    continue

            # 4. Время истекло (12 часов).
            # Продаем только если мы около нуля, чтобы не сидеть в мертвой монете вечно.
            if hold_time > timedelta(hours=12):
                if Decimal("0.98") < pnl_ratio < Decimal("1.02"):
                    positions_to_sell.append(
                        (
                            symbol,
                            position,
                            current_price,
                            f"ВРЕМЯ ИСТЕКЛО ({hold_time})",
                        )
                    )

        return positions_to_sell

    def find_optimized_opportunities(self, tickers, portfolio):
        """
        Поиск возможностей с COOLDOWN фильтром.
        """
        opportunities = []

        # 1. Получаем список монет, проданных за последние 60 минут
        recent_sells = set()
        if self.db_conn:
            try:
                with self.db_conn.cursor() as cur:
                    cur.execute("""
                        SELECT symbol FROM portfolio
                        WHERE status = 'closed'
                        AND exit_time > NOW() - INTERVAL '60 minutes'
                    """)
                    recent_sells = {row[0] for row in cur.fetchall()}
            except Exception as e:
                logger.error(f"Ошибка получения истории продаж: {e}")

        current_categories = self.analyze_portfolio_diversification(portfolio, tickers)
        logger.info("🔍 АНАЛИЗ РЫНОЧНЫХ ВОЗМОЖНОСТЕЙ...")

        for symbol, data in tickers.items():
            if symbol in portfolio:
                continue

            # === COOLDOWN ФИЛЬТР ===
            if symbol in recent_sells:
                # Логируем только если это топ монета, чтобы не спамить
                if data.get("score", 0) > 8:
                    logger.info(f"❄️ Cooldown: пропускаем {symbol} (недавно продана)")
                continue

            score = data.get("score", Decimal("0"))
            price = data["price"]

            if data["volume"] < Decimal("50000"):
                continue

            atr = self.calculate_atr(symbol)
            if atr > Decimal("0.15"):
                continue

            category = "unknown"
            if price < Decimal("0.01"):
                category = "micro_cap"
            elif price < Decimal("1"):
                category = "low_cap"
            elif price < Decimal("10"):
                category = "mid_cap"
            else:
                category = "high_cap"

            diversification_bonus = Decimal("0")
            if current_categories.get(category, 0) == 0:
                diversification_bonus = Decimal("3")
            elif current_categories.get(category, 0) <= 1:
                diversification_bonus = Decimal("1")

            final_score = score + diversification_bonus
            opportunities.append((symbol, final_score, price, category))

        opportunities.sort(key=lambda x: x[1], reverse=True)
        logger.info(f"   Найдено возможностей (после фильтров): {len(opportunities)}")
        return opportunities[:10]

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
'''


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

        # --- ПАРАМЕТРЫ КАПИТАЛА ---
        self.total_capital = Decimal("20")
        self.kelly_manager = KellyCapitalManagement(self.total_capital)
        self.performance_analytics = PerformanceAnalytics()
        self.smart_order_manager = SmartOrderManager(self.exchange)

        # Интервалы
        self.rebalance_interval = 300  # 5 минут
        self.tickers_cache_ttl = 60

        # Настройки позиций (будут перезаписаны в auto_adjust_parameters)
        self.max_positions = 1
        self.min_position_size = Decimal("5")
        self.max_position_size = Decimal("15")
        self.reserve_cash = Decimal("2")

        # --- ПАРАМЕТРЫ СТРАТЕГИИ (Базовые) ---
        self.initial_stop_atr = Decimal("2.0")
        self.take_profit_atr = Decimal("3.0")
        self.min_order = Decimal("5")
        self.commission = Decimal("0.001")

        # --- STABLECOINS ---
        self.STABLECOINS = [
            "USDC",
            "TUSD",
            "FDUSD",
            "USDD",
            "BUSD",
            "DAI",
            "PAX",
            "GUSD",
            "EURT",
        ]

        # Кэш
        self.atr_cache = {}
        self.atr_cache_ttl = 3600
        self.last_tickers_update = None
        self.cached_tickers = {}
        self.last_status_log = 0
        self.status_log_interval = 60

        # Трейлинг и защита
        self.trailing_stop_max_prices = {}

        # БД
        self.db_config = {
            "host": os.getenv("DB_HOST", "127.0.0.1"),
            "database": os.getenv("DB_NAME", "dobyt"),
            "user": os.getenv("DB_USER", "trading_user"),
            "password": os.getenv("DB_PASSWORD", "bitpa$$w0rd"),
            "port": os.getenv("DB_PORT", "5432"),
        }

        self.db_conn = self.init_db()
        if self.db_conn:
            self.cleanup_invalid_symbols()
            self.cleanup_dust_positions()
            logger.info("🔄 Первоначальная синхронизация...")
            self.sync_portfolio_with_exchange()
        else:
            logger.warning("⚠️ Работа без БД!")

        if not self.health_check():
            raise Exception("Health check failed")

    def init_db(self):
        """Инициализация БД"""
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
            conn.commit()
            return conn
        except Exception as e:
            logger.error(f"❌ Ошибка БД: {e}")
            return None

    # --- МЕТОД 1: BTC FILTER (Рыночный режим) ---
    def get_market_regime(self):
        """
        Определяет состояние рынка по BTC.
        Returns: 'bull', 'bear', 'danger' (перегрет).
        """
        try:
            # Качаем BTC 4h (глобальный тренд)
            ohlcv = self.exchange.fetch_ohlcv("BTC/USDT", "4h", limit=200)
            if not ohlcv or len(ohlcv) < 100:
                return "bull"

            closes = [float(x[4]) for x in ohlcv]
            df = pd.Series(closes)

            # EMA 200
            ema_200 = df.ewm(span=200, adjust=False).mean().iloc[-1]
            current_price = closes[-1]

            # RSI 14
            delta = df.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss.replace(0, 0.0000001)
            rsi = 100 - (100 / (1 + rs))
            rsi_val = rsi.iloc[-1]

            if rsi_val > 80:
                return "danger"  # BTC перегрет
            elif current_price < ema_200:
                return "bear"  # Нисходящий тренд
            else:
                return "bull"  # Восходящий тренд
        except Exception as e:
            logger.error(f"Ошибка анализа BTC: {e}")
            return "bull"

    def calculate_atr(self, symbol, period=14):
        try:
            cache_key = f"{symbol}_{period}"
            if cache_key in self.atr_cache:
                ts, val = self.atr_cache[cache_key]
                if time.time() - ts < self.atr_cache_ttl:
                    return val

            ohlcv = self.exchange.fetch_ohlcv(symbol, "1d", limit=period + 5)
            if len(ohlcv) < period + 1:
                return Decimal("0.05")

            highs = np.array([float(x[2]) for x in ohlcv])
            lows = np.array([float(x[3]) for x in ohlcv])
            closes = np.array([float(x[4]) for x in ohlcv])

            tr = np.maximum(
                highs[1:] - lows[1:],
                np.maximum(
                    np.abs(highs[1:] - closes[:-1]), np.abs(lows[1:] - closes[:-1])
                ),
            )
            atr = np.mean(tr[-period:])
            atr_pct = Decimal(str(atr / closes[-1]))

            self.atr_cache[cache_key] = (time.time(), atr_pct)
            return atr_pct
        except:
            return Decimal("0.05")

    def calculate_rsi(self, symbol, period=14):
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, "15m", limit=100)
            if len(ohlcv) < period + 1:
                return Decimal("50")
            closes = [float(x[4]) for x in ohlcv]
            df = pd.Series(closes)
            delta = df.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss.replace(0, 0.0000001)
            rsi = 100 - (100 / (1 + rs))
            return Decimal(str(rsi.iloc[-1]))
        except:
            return Decimal("50")

    # --- МЕТОД 2 & 3: SCORING ENGINE (OBV + Bollinger) ---
    def calculate_advanced_score(self, ticker_data):
        """Мульти-факторный скоринг: Momentum + OBV + Bollinger."""
        try:
            symbol = ticker_data["symbol"]

            # Качаем данные (1h таймфрейм для анализа структуры)
            ohlcv = self.exchange.fetch_ohlcv(symbol, "1h", limit=100)
            if len(ohlcv) < 50:
                return Decimal("0")

            df = pd.DataFrame(
                ohlcv, columns=["time", "open", "high", "low", "close", "volume"]
            )
            current_price = df["close"].iloc[-1]

            # 1. Momentum (Цена растет?)
            closes = df["close"]
            ema_20 = closes.ewm(span=20, adjust=False).mean().iloc[-1]
            mom_score = Decimal("5")
            if current_price > ema_20:
                mom_score = Decimal("8")
            else:
                mom_score = Decimal("3")

            # 2. OBV (Деньги заходят?)
            obv = (np.sign(df["close"].diff()) * df["volume"]).fillna(0).cumsum()
            obv_sma = obv.rolling(20).mean().iloc[-1]
            current_obv = obv.iloc[-1]

            obv_score = Decimal("5")
            if current_obv > obv_sma:
                obv_score = Decimal("8")  # Деньги заходят
            elif current_obv < obv_sma:
                obv_score = Decimal("3")  # Деньги выходят

            # 3. Bollinger Bands (Перекупленность?)
            sma = closes.rolling(20).mean()
            std = closes.rolling(20).std()
            upper_bb = (sma + 2 * std).iloc[-1]
            lower_bb = (sma - 2 * std).iloc[-1]

            bb_score = Decimal("5")
            if current_price > upper_bb:
                bb_score = Decimal("2")  # Слишком дорого, риск отката
            elif current_price < lower_bb:
                bb_score = Decimal("4")  # Дешево, но может быть падение
            elif current_price > sma.iloc[-1]:
                bb_score = Decimal("7")  # В верхней половине, здоровый тренд

            # Взвешенная сумма
            # Momentum 40%, OBV 30%, BB 30%
            final_score = (
                (mom_score * Decimal("0.4"))
                + (obv_score * Decimal("0.3"))
                + (bb_score * Decimal("0.3"))
            )

            # Дополнительный буст от ticker change_24h (из аргументов)
            change_boost = Decimal("0")
            if ticker_data.get("change_24h", 0) > 0.05:
                change_boost = Decimal("1")  # +1 балл за сильный рост

            return final_score + change_boost

        except Exception as e:
            logger.debug(f"Ошибка скоринга {symbol}: {e}")
            return Decimal("0")

    def get_usdt_balance(self):
        """Получение баланса USDT"""
        try:
            balance = self.exchange.fetch_balance(params={"type": "spot"})
            # Ищем USDT в разных местах
            if "free" in balance and "USDT" in balance["free"]:
                return Decimal(str(balance["free"]["USDT"]))
            if "USDT" in balance and "free" in balance["USDT"]:
                return Decimal(str(balance["USDT"]["free"]))
            return Decimal("0")
        except Exception as e:
            logger.error(f"❌ Ошибка баланса: {e}")
            return Decimal("0")

    def get_current_portfolio(self):
        portfolio = {}
        if self.db_conn:
            with self.db_conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol, quantity, entry_price, entry_time, current_price FROM portfolio WHERE status = 'active'"
                )
                for row in cur.fetchall():
                    symbol, qty, ep, et, cp = row
                    portfolio[symbol] = {
                        "quantity": Decimal(str(qty)),
                        "entry_price": Decimal(str(ep)),
                        "entry_time": et,
                        "current_price": Decimal(str(cp if cp else 0)),
                    }
        return portfolio

    def sync_portfolio_with_exchange(self):
        try:
            balance = self.exchange.fetch_balance(params={"type": "spot"})
            if not self.db_conn:
                return

            db_active = set()
            with self.db_conn.cursor() as cur:
                cur.execute("SELECT symbol FROM portfolio WHERE status = 'active'")
                db_active = {row[0] for row in cur.fetchall()}

            real_balances = {}
            for curr, data in balance.items():
                if curr in [
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
                    total = Decimal(str(data.get("total", 0)))
                    if total > 0:
                        real_balances[f"{curr}/USDT"] = total

            # Sync Logic
            with self.db_conn.cursor() as cur:
                for sym, qty in real_balances.items():
                    # Получаем цену
                    try:
                        ticker = self.exchange.fetch_ticker(sym.replace("/", ""))
                        price = Decimal(str(ticker["last"]))
                    except:
                        price = Decimal("0")

                    val = qty * price
                    if val > Decimal("2"):  # Реальная позиция
                        cur.execute(
                            """
                            INSERT INTO portfolio (symbol, quantity, entry_price, current_price, status)
                            VALUES (%s, %s, %s, %s, 'active')
                            ON CONFLICT (symbol) WHERE status = 'active'
                            DO UPDATE SET quantity = EXCLUDED.quantity, current_price = EXCLUDED.current_price
                        """,
                            (sym, float(qty), float(price), float(price)),
                        )
                    elif sym in db_active:  # Пыль в БД
                        cur.execute(
                            "UPDATE portfolio SET status = 'closed' WHERE symbol = %s",
                            (sym,),
                        )
                        logger.info(f"🧹 Убрана пыль {sym}")

                for sym in db_active:
                    if sym not in real_balances:
                        cur.execute(
                            "UPDATE portfolio SET status = 'closed' WHERE symbol = %s",
                            (sym,),
                        )

            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}")

    def auto_adjust_parameters(self):
        """Адаптация под баланс"""
        try:
            real_balance = self.get_usdt_balance()
            active_pos = len(self.get_current_portfolio())

            if real_balance < Decimal("20"):
                self.max_positions = 1
                self.reserve_cash = Decimal("1")
                available = real_balance - self.reserve_cash

                # Sniper Mode: Если есть позиция - стоп. Если нет - All in.
                if active_pos >= 1:
                    self.min_position_size = Decimal("999999")
                else:
                    self.min_position_size = max(Decimal("5.5"), available)

            elif real_balance < Decimal("80"):
                self.max_positions = 2
                self.reserve_cash = Decimal("2")
                share = (real_balance - self.reserve_cash) / 2
                self.min_position_size = share * Decimal("0.9")

            else:
                self.max_positions = 3
                self.reserve_cash = Decimal("5")
                share = (real_balance - self.reserve_cash) / 3
                self.min_position_size = share * Decimal("0.9")

        except Exception as e:
            logger.error(f"Ошибка настройки: {e}")

    # --- МЕТОД 4 & 5: TIME DECAY + SMART DCA ---
    def check_stop_conditions(self, portfolio, tickers):
        """Проверка условий выхода (Time Decay) и докупки (DCA)."""
        positions_to_sell = []
        dca_candidates = []  # Кандидаты на докупку

        for symbol, position in portfolio.items():
            if symbol in tickers:
                current_price = tickers[symbol]["price"]
            else:
                try:
                    ticker = self.exchange.fetch_ticker(symbol.replace("/", ""))
                    current_price = Decimal(str(ticker["last"]))
                except:
                    continue

            entry_price = position["entry_price"]
            entry_time = position.get("entry_time") or datetime.now()

            # Исправление таймзоны
            now = datetime.now()
            delta = now - entry_time
            # Если дельта странная (отрицательная или > 30 дней для новой), сбрасываем
            if delta.total_seconds() < 0 or delta.days > 30:
                hours_held = 0
            else:
                hours_held = delta.total_seconds() / 3600

            position_value = position["quantity"] * current_price
            if position_value < Decimal("1"):
                continue

            pnl_ratio = current_price / entry_price
            pnl_pct = (pnl_ratio - 1) * 100

            # --- 1. SMART DCA (Только для депозитов > $80) ---
            # Если упали на -5%...-10%, RSI перепродан, и у нас есть деньги
            if self.get_usdt_balance() > Decimal("40") and self.max_positions > 1:
                if pnl_ratio < Decimal("0.95") and pnl_ratio > Decimal("0.85"):
                    rsi = self.calculate_rsi(symbol)
                    if rsi < Decimal("35"):  # Перепроданность
                        logger.info(
                            f"💡 Сигнал DCA для {symbol}: Просадка {pnl_pct:.2f}%, RSI {rsi}"
                        )
                        # Мы не добавляем в positions_to_sell, мы выходим (hold)
                        # В полной версии тут вызывался бы buy, но пока просто держим
                        continue

            # --- 2. TIME-BASED DECAY (Динамический тейк) ---
            # Чем дольше держим, тем меньше хотим прибыли
            target_profit = Decimal("1.08")  # База 8%

            if hours_held > 2:
                target_profit = Decimal("1.05")  # 5%
            if hours_held > 6:
                target_profit = Decimal("1.02")  # 2%
            if hours_held > 12:
                target_profit = Decimal("1.005")  # 0.5% (выход в 0)
            if hours_held > 24:
                target_profit = Decimal("0.99")  # -1% (надоело ждать)

            # --- ПРОВЕРКИ ВЫХОДА ---

            # Тейк-профит (Динамический)
            if pnl_ratio >= target_profit:
                reason = (
                    f"ТЕЙК (Time-Decay: >{int(hours_held)}h)"
                    if hours_held > 2
                    else "ТЕЙК-ПРОФИТ"
                )
                positions_to_sell.append(
                    (symbol, position, current_price, f"{reason} ({pnl_pct:.2f}%)")
                )
                continue

            # Стоп-лосс (Фиксированный -6%)
            if pnl_ratio <= Decimal("0.94"):
                positions_to_sell.append(
                    (symbol, position, current_price, f"СТОП-ЛОСС ({pnl_pct:.2f}%)")
                )
                continue

            # Трейлинг (Если выросли > 3%)
            if pnl_ratio > Decimal("1.03"):
                if symbol not in self.trailing_stop_max_prices:
                    self.trailing_stop_max_prices[symbol] = current_price
                else:
                    if current_price > self.trailing_stop_max_prices[symbol]:
                        self.trailing_stop_max_prices[symbol] = current_price

                trigger = self.trailing_stop_max_prices[symbol] * Decimal("0.98")
                if current_price <= trigger:
                    positions_to_sell.append(
                        (symbol, position, current_price, "ТРЕЙЛИНГ-СТОП")
                    )
                    continue

        return positions_to_sell

    def find_optimized_opportunities(self, tickers, portfolio):
        """Поиск с учетом Cooldown."""
        opportunities = []

        # Cooldown (не покупать проданное 60 мин)
        recent_sells = set()
        if self.db_conn:
            with self.db_conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol FROM portfolio WHERE status='closed' AND exit_time > NOW() - INTERVAL '60 minutes'"
                )
                recent_sells = {row[0] for row in cur.fetchall()}

        categories = self.analyze_portfolio_diversification(portfolio, tickers)

        logger.info("🔍 АНАЛИЗ РЫНОЧНЫХ ВОЗМОЖНОСТЕЙ...")

        for symbol, data in tickers.items():
            if symbol in portfolio:
                continue
            if symbol in recent_sells:
                continue  # Cooldown

            score = data.get("score", Decimal("0"))
            price = data["price"]

            # Базовые фильтры
            if data["volume"] < Decimal("50000"):
                continue
            if self.calculate_atr(symbol) > Decimal("0.15"):
                continue

            # Категории
            cat = "high_cap"
            if price < 0.01:
                cat = "micro_cap"
            elif price < 10:
                cat = "mid_cap"

            bonus = Decimal("0")
            if categories.get(cat, 0) == 0:
                bonus = Decimal("2")

            opportunities.append((symbol, score + bonus, price, cat))

        opportunities.sort(key=lambda x: x[1], reverse=True)
        logger.info(f"   Найдено возможностей: {len(opportunities)}")
        return opportunities[:15]

    def analyze_portfolio_diversification(self, portfolio, tickers):
        counts = {}
        for s, p in portfolio.items():
            # Упрощенная логика категорий
            counts["any"] = counts.get("any", 0) + 1
        return counts

    def get_cached_tickers(self):
        if (
            not self.cached_tickers
            or time.time() - (self.last_tickers_update or 0) > 60
        ):
            self.cached_tickers = self.safe_fetch_filtered_tickers()
            self.last_tickers_update = time.time()
        return self.cached_tickers

    def safe_fetch_filtered_tickers(self):
        try:
            tickers = self.exchange.fetch_tickers()
            candidates = []
            for s, t in tickers.items():
                if not s.endswith("/USDT"):
                    continue
                if t["quoteVolume"] is None or t["last"] is None:
                    continue

                vol = Decimal(str(t["quoteVolume"]))
                if vol < Decimal("50000"):
                    continue

                change = Decimal(str(t.get("percentage", 0)))
                price = Decimal(str(t["last"]))

                candidates.append(
                    {"symbol": s, "price": price, "volume": vol, "change_24h": change}
                )

            # Отбираем топ-40 по объему и росту
            candidates.sort(key=lambda x: x["volume"], reverse=True)
            top_vol = candidates[:40]
            candidates.sort(key=lambda x: x["change_24h"], reverse=True)
            top_gain = candidates[:30]

            unique = {c["symbol"]: c for c in top_vol + top_gain}.values()
            filtered = {}

            for cand in unique:
                time.sleep(0.1)  # Rate limit
                score = self.calculate_advanced_score(cand)
                if score > Decimal("5"):
                    cand["score"] = score
                    filtered[cand["symbol"]] = cand
                    logger.info(f"   ⭐ {cand['symbol']}: Score {score:.1f}")

            return filtered
        except Exception as e:
            logger.error(f"Ошибка тикеров: {e}")
            return {}

    def cleanup_invalid_symbols(self):
        pass  # Заглушка, логика есть в sync

    def cleanup_dust_positions(self):
        pass  # Логика перенесена в sync

    def cleanup_old_cache(self):
        curr = time.time()
        keys = [
            k for k, v in self.atr_cache.items() if curr - v[0] > self.atr_cache_ttl
        ]
        for k in keys:
            del self.atr_cache[k]

    def health_check(self):
        return True

    def log_initial_portfolio(self):
        p = self.get_current_portfolio()
        logger.info(f"📊 Портфель: {len(p)} позиций")

    # --- MAIN LOOP ---
    def enhanced_rebalance(self, iteration):
        """
        Двухскоростная ребалансировка:
        - Проверка стопов: Каждую итерацию (быстро).
        - Поиск новых монет: Только раз в 5 минут (медленно).
        """
        try:
            # 1. СИНХРОНИЗАЦИЯ И БАЛАНС (Делаем всегда)
            # Это легкие запросы, можно делать часто
            self.sync_portfolio_with_exchange()
            balance = self.get_usdt_balance()
            portfolio = self.get_current_portfolio()

            # Определяем, нужно ли запускать "Тяжелый сканер"
            # Запускаем сканер, только если прошло 300 секунд с прошлого раза
            # ИЛИ если это первый запуск
            current_time = time.time()
            last_scan = getattr(self, "last_scan_time", 0)
            should_scan = (current_time - last_scan) > 300 or iteration == 1

            # Логирование статуса (каждую итерацию, чтобы видеть динамику)
            real_pos = []
            for s, p in portfolio.items():
                val = p["quantity"] * p["current_price"]
                if val > 2:
                    real_pos.append(s)

            logger.info(
                f"📊 СТАТУС: Баланс {balance} | Позиций {len(real_pos)}/{self.max_positions}"
            )
            for s in real_pos:
                p = portfolio[s]
                entry = p["entry_price"]
                curr = p["current_price"]
                # Защита от деления на ноль
                if entry > 0:
                    pnl = (curr / entry - 1) * 100
                else:
                    pnl = Decimal("0")

                # Добавим RSI в лог для понимания ситуации
                rsi = self.calculate_rsi(s)
                logger.info(f"   💎 {s}: {pnl:.2f}% | Цена {curr} | RSI {rsi:.1f}")

            # --- БЛОК 1: ПРОДАЖА (ЗАЩИТА) ---
            # Запускается КАЖДУЮ итерацию (каждые 20 сек)
            # Нам нужны актуальные цены ТОЛЬКО для наших монет
            my_tickers = {}
            for symbol in real_pos:
                try:
                    t = self.exchange.fetch_ticker(symbol.replace("/", ""))
                    my_tickers[symbol] = {"price": Decimal(str(t["last"]))}
                except:
                    pass

            to_sell = self.check_stop_conditions(portfolio, my_tickers)

            for sym, pos, price, reason in to_sell:
                logger.info(f"🔻 Продажа {sym}: {reason}")
                if self.smart_order_manager.execute_smart_sell(
                    sym, pos["quantity"], price
                ):
                    q = float(pos["quantity"])
                    ep = float(pos["entry_price"])
                    cp = float(price)
                    pnl = (cp - ep) * q

                    with self.db_conn.cursor() as cur:
                        cur.execute(
                            """
                                UPDATE portfolio SET status='closed', exit_price=%s, exit_time=NOW(), profit_loss=%s
                                WHERE symbol=%s AND status='active'
                            """,
                            (cp, pnl, sym),
                        )
                    self.db_conn.commit()
                    logger.info(f"✅ Закрыто: {sym} (PnL {pnl:.4f})")

                    # Обновляем данные сразу после продажи
                    balance = self.get_usdt_balance()
                    portfolio = self.get_current_portfolio()
                    # Если продали, можно сразу разрешить поиск, не ожидая таймера
                    should_scan = True

            # --- БЛОК 2: ПОКУПКА (СКАНИРОВАНИЕ) ---
            # Запускается РЕДКО (раз в 5 минут) или если освободился слот

            # Есть ли смысл сканировать? (Есть деньги и слоты)
            busy_slots = len(
                [
                    k
                    for k, v in portfolio.items()
                    if v["quantity"] * v["current_price"] > 2
                ]
            )
            self.auto_adjust_parameters()  # Обновим лимиты

            can_buy = (
                busy_slots < self.max_positions and balance > self.min_position_size
            )

            if can_buy and should_scan:
                logger.info("🔍 Запуск сканера рынка (ищем новые монеты)...")

                # Обновляем глобальные тикеры только здесь (ТЯЖЕЛАЯ ОПЕРАЦИЯ)
                self.cached_tickers = self.safe_fetch_filtered_tickers()
                self.last_scan_time = time.time()
                tickers = self.cached_tickers

                # Проверка рынка
                market_status = self.get_market_regime()
                logger.info(f"🌍 Рынок (BTC): {market_status}")

                if market_status == "danger":
                    logger.info("🔥 Рынок перегрет, пропускаем цикл покупки")
                    return True

                opps = self.find_optimized_opportunities(tickers, portfolio)

                for sym, score, price, cat in opps:
                    # Повторные проверки перед входом
                    if self.calculate_rsi(sym) > Decimal("75"):
                        continue
                    if market_status == "bear" and score < Decimal("8"):
                        continue

                    amount = self.min_position_size
                    if self.max_positions == 1:
                        amount = balance - self.reserve_cash

                    logger.info(f"🛒 Покупка {sym} ({amount:.2f} USDT)")
                    if self.smart_order_manager.execute_smart_buy(sym, amount):
                        logger.info("✅ Куплено")
                        break  # 1 покупка за цикл

            elif not can_buy and should_scan:
                # Если сканировать пора, но нет мест - просто обновим время,
                # чтобы не пытаться сканировать каждую следующую секунду
                self.last_scan_time = time.time()
                logger.info("💤 Нет мест для покупки, сканирование отложено.")

            if iteration % 288 == 0:
                self.performance_analytics.generate_performance_report()

            self.cleanup_old_cache()
            return True

        except Exception as e:
            logger.error(f"Ошибка цикла: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return False

    def run_optimized(self):
        """Основной цикл работы бота"""
        logger.info("🚀 Запуск Fast-Response бота Bybit")

        try:
            balance = self.get_usdt_balance()
            if balance <= Decimal("0"):
                p = self.get_current_portfolio()
                if not p:
                    logger.error("❌ Баланс 0 и портфель пуст. Пополните депозит.")
                    return
        except Exception as e:
            logger.error(f"Ошибка инициализации: {e}")

        iteration = 0
        consecutive_errors = 0

        # Инициализируем время сканирования в прошлом, чтобы первый запуск сработал сразу
        self.last_scan_time = 0

        while True:
            try:
                iteration += 1

                if not self.db_conn or self.db_conn.closed:
                    logger.info("🔄 Переподключение к БД...")
                    self.db_conn = self.init_db()

                success = self.enhanced_rebalance(iteration)

                if success:
                    consecutive_errors = 0
                    # === ИЗМЕНЕНИЕ ЗДЕСЬ ===
                    # Ждем всего 20 секунд вместо 300.
                    # В enhanced_rebalance стоит логика, которая не даст
                    # спамить сканированием рынка, но позволит проверить стопы.
                    wait_time = 20
                    logger.info(
                        f"⏳ Мониторинг... (след. проверка через {wait_time} сек)"
                    )
                    time.sleep(wait_time)
                else:
                    consecutive_errors += 1
                    sleep_time = 60 if consecutive_errors < 3 else 300
                    logger.warning(f"⚠️ Ошибка. Пауза {sleep_time} сек.")
                    time.sleep(sleep_time)

            except KeyboardInterrupt:
                logger.info("\n⏹️ Остановка...")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка main loop: {e}")
                time.sleep(60)


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
