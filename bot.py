import json
import logging
import math
import os
import time
from datetime import datetime, timedelta
from decimal import Decimal

import ccxt
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

        # Конфигурация
        self.total_capital = Decimal("50")
        self.min_order = Decimal("5")
        self.commission = Decimal("0.001")
        self.max_positions = 3
        self.position_size = Decimal("12")
        self.reserve_cash = Decimal("8")
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

        # NEW: Хранилище для трейлинг-стопа
        self.trailing_stop_max_prices = {}

        # Параметры БД
        self.db_config = {
            "host": os.getenv("DB_HOST", "postgres"),
            "database": os.getenv("DB_NAME", "bybit_bot"),
            "user": os.getenv("DB_USER", "trader"),
            "password": os.getenv("DB_PASSWORD", "trading_password"),
            "port": os.getenv("DB_PORT", "5432"),
        }

        # Кэширование данных для оптимизации
        self.last_tickers_update = None
        self.cached_tickers = {}
        self.tickers_cache_ttl = 10
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

    def init_db(self):
        """Инициализация PostgreSQL соединения с проверкой и добавлением отсутствующих колонок"""
        try:
            conn = psycopg2.connect(**self.db_config)
            with conn.cursor() as cur:
                # Создаем таблицу portfolio если не существует
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

                # 🔴 ДОБАВЛЕНО: Проверяем и добавляем отсутствующие колонки
                # Проверяем наличие колонки exit_time
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'portfolio' AND column_name = 'exit_time'
                """)
                if not cur.fetchone():
                    cur.execute("ALTER TABLE portfolio ADD COLUMN exit_time TIMESTAMP")
                    logger.info("✅ Добавлена отсутствующая колонка: exit_time")

                # Проверяем наличие колонки exit_price
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'portfolio' AND column_name = 'exit_price'
                """)
                if not cur.fetchone():
                    cur.execute(
                        "ALTER TABLE portfolio ADD COLUMN exit_price DECIMAL(20,8)"
                    )
                    logger.info("✅ Добавлена отсутствующая колонка: exit_price")

                # Проверяем наличие колонки profit_loss
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'portfolio' AND column_name = 'profit_loss'
                """)
                if not cur.fetchone():
                    cur.execute(
                        "ALTER TABLE portfolio ADD COLUMN profit_loss DECIMAL(10,4)"
                    )
                    logger.info("✅ Добавлена отсутствующая колонка: profit_loss")

                # Проверяем наличие колонки status
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'portfolio' AND column_name = 'status'
                """)
                if not cur.fetchone():
                    cur.execute(
                        "ALTER TABLE portfolio ADD COLUMN status VARCHAR(10) DEFAULT 'active'"
                    )
                    logger.info("✅ Добавлена отсутствующая колонка: status")

                # Создаем уникальный индекс
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_active_symbol
                    ON portfolio (symbol)
                    WHERE status = 'active';
                """)

                # Создаем таблицу transactions
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
            logger.info("✅ База данных инициализирована и проверена")
            return conn
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            return None

    def cleanup_invalid_symbols(self):
        """Очистка позиций с неправильным форматом символов из БД"""
        try:
            if not self.db_conn:
                return

            with self.db_conn.cursor() as cur:
                # Находим позиции с неправильным форматом символов
                cur.execute("""
                    SELECT symbol FROM portfolio
                    WHERE status = 'active' AND (symbol LIKE '%:%' OR symbol NOT LIKE '%/%')
                """)
                invalid_symbols = [row[0] for row in cur.fetchall()]

                # Помечаем их как закрытые
                for symbol in invalid_symbols:
                    cur.execute(
                        "UPDATE portfolio SET status = 'closed' WHERE symbol = %s",
                        (symbol,),
                    )

                self.db_conn.commit()
                logger.info(
                    f"🧹 Очищено позиций с неправильным форматом: {len(invalid_symbols)}"
                )

        except Exception as e:
            logger.error(f"❌ Ошибка очистки неправильных символов: {e}")

    def log_initial_portfolio(self):
        """Логирование начального состояния портфеля"""
        try:
            portfolio = self.get_current_portfolio()
            balance = self.get_usdt_balance()

            logger.info("📊 НАЧАЛЬНОЕ СОСТОЯНИЕ ПОРТФЕЛЯ:")
            logger.info(f"💰 Баланс USDT: {balance}")
            logger.info(f"📦 Позиций в портфеле: {len(portfolio)}")

            if portfolio:
                logger.info("🔍 Детали портфеля:")
                total_value = Decimal("0")
                for symbol, position in portfolio.items():
                    current_price = position.get("current_price", Decimal("0"))
                    quantity = position["quantity"]
                    value = quantity * current_price
                    total_value += value
                    logger.info(
                        f"   {symbol}: {quantity} × {current_price} = {value:.2f} USDT"
                    )

                logger.info(f"📈 Общая стоимость портфеля: {total_value:.2f} USDT")
            else:
                logger.info("💡 Портфель пуст")

        except Exception as e:
            logger.error(f"❌ Ошибка логирования начального портфеля: {e}")

    def get_cached_tickers(self):
        """Получение тикеров с кэшированием"""
        current_time = time.time()

        if (
            self.last_tickers_update is None
            or current_time - self.last_tickers_update > self.tickers_cache_ttl
            or not self.cached_tickers
        ):
            try:
                old_count = len(self.cached_tickers)
                self.cached_tickers = self.enhanced_fetch_filtered_tickers()
                self.last_tickers_update = current_time

                new_count = len(self.cached_tickers)
                logger.debug(f"🔄 Обновлен кэш тикеров: {old_count} -> {new_count} пар")

            except Exception as e:
                logger.error(f"❌ Ошибка обновления кэша тикеров: {e}")

        return self.cached_tickers

    def enhanced_fetch_filtered_tickers(self):
        """Исправленная загрузка тикеров с реалистичными фильтрами"""
        try:
            tickers = self.exchange.fetch_tickers()
            logger.info(f"📈 Загружено тикеров: {len(tickers)}")
            filtered = {}
            # 🔴 ФИКС: РЕАЛИСТИЧНЫЙ ПОРОГ ОБЪЕМА - 10000 USDT вместо 1 млн
            MIN_24H_VOLUME = Decimal("10000")  # 10 тысяч USDT
            # 🔴 ФИКС: ДИАГНОСТИКА - логируем примеры тикеров
            sample_tickers = list(tickers.items())[
                :3
            ]  # Первые 3 тикера для диагностики
            for symbol, ticker in sample_tickers:
                logger.info(
                    f"🔍 Пример тикера {symbol}: last={ticker.get('last')}, quoteVolume={ticker.get('quoteVolume')}"
                )
            for symbol, ticker in tickers.items():
                try:
                    # 🔴 ФИКС: Правильная фильтрация USDT пар
                    if not symbol.endswith("/USDT") and not symbol.endswith("USDT"):
                        continue
                    # NEW: Смягчённый фильтр (только если нужно, e.g. пропуск дат вроде 2511/2512)
                    if any(
                        x in symbol for x in ["2511", "2512"]
                    ):  # Только проблемные даты, если они есть
                        logger.debug(f"⚠️ Пропущен символ с датой: {symbol}")
                        continue
                    # 🔴 ФИКС: Безопасное получение данных
                    last_price = ticker.get("last")
                    quote_volume = ticker.get("quoteVolume")
                    if last_price is None or quote_volume is None:
                        continue
                    price = Decimal(str(last_price))
                    volume = Decimal(str(quote_volume))
                    # 🔴 ФИКС: РЕАЛИСТИЧНЫЕ ФИЛЬТРЫ
                    if volume < MIN_24H_VOLUME:
                        continue
                    if price <= Decimal("0") or price > Decimal(
                        "100000"
                    ):  # NEW: Подняли max для BTC (~90k)
                        continue
                    # Единый формат символов
                    if "/" in symbol:
                        ccxt_symbol = symbol
                        base_symbol = symbol.replace("/USDT", "")
                    else:
                        base_symbol = symbol.replace("USDT", "")
                        ccxt_symbol = f"{base_symbol}/USDT"
                    if not base_symbol:
                        continue
                    if base_symbol in self.STABLECOINS:  # NEW: Пропускаем стэйблкоины
                        logger.debug(f"⚠️ Пропущен стэйблкоин: {symbol}")
                        continue

                    enhanced_score = self.calculate_enhanced_growth_score(
                        {
                            "price": price,
                            "volume": volume,
                            "change_24h": Decimal(str(ticker.get("percentage", 0))),
                            "symbol": base_symbol,
                        }
                    )
                    filtered[ccxt_symbol] = {
                        "price": price,
                        "volume": volume,
                        "change_24h": Decimal(str(ticker.get("percentage", 0))),
                        "symbol": ccxt_symbol,
                        "base_symbol": base_symbol,
                        "score": enhanced_score,
                        "category": self.categorize_asset(
                            base_symbol, price, volume, Decimal("0")
                        ),
                    }
                except Exception as e:
                    logger.debug(f"⚠️ Пропущен тикер {symbol}: {e}")
                    continue
            # 🔴 ФИКС: ДЕТАЛЬНАЯ СТАТИСТИКА
            logger.info("🎯 СТАТИСТИКА РЫНКА:")
            logger.info(f" Загружено тикеров: {len(tickers)}")
            logger.info(f" Отфильтровано пар: {len(filtered)}")
            logger.info(f" Минимальный объем: {MIN_24H_VOLUME} USDT")
            if filtered:
                volumes = [data["volume"] for data in filtered.values()]
                avg_volume = sum(volumes) / len(volumes)
                max_volume = max(volumes)
                logger.info(f" Средний объем: {avg_volume:.0f} USDT")
                logger.info(f" Максимальный объем: {max_volume:.0f} USDT")
                # Показываем топ-5 пар по объему
                top_pairs = sorted(
                    filtered.items(), key=lambda x: x[1]["volume"], reverse=True
                )[:5]
                logger.info(" Топ-5 пар по объему:")
                for pair, data in top_pairs:
                    logger.info(f" {pair}: {data['volume']:.0f} USDT")
            return filtered
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки тикеров: {e}")
            return {}

    def calculate_enhanced_growth_score(self, ticker_data):
        """Расчет скора роста с реалистичными параметрами"""
        try:
            change = ticker_data["change_24h"]
            volume = ticker_data["volume"]
            price = ticker_data["price"]

            # 🔴 ФИКС: РЕАЛИСТИЧНЫЕ КОЭФФИЦИЕНТЫ
            price_score = change * Decimal("0.5")  # Уменьшил влияние изменения цены
            volume_factor = Decimal(
                math.log(max(float(volume), 1000) / 1000)
            )  # Меньший порог
            volume_score = volume_factor * Decimal("3")  # Уменьшил вес объема

            # 🔴 ФИКС: РЕАЛИСТИЧНЫЕ БОНУСЫ ЗА ЛИКВИДНОСТЬ
            liquidity_bonus = Decimal("0")
            if volume > Decimal("1000000"):  # 1 миллион USDT
                liquidity_bonus = Decimal("10")
            elif volume > Decimal("100000"):  # 100 тысяч USDT
                liquidity_bonus = Decimal("5")
            elif volume > Decimal("10000"):  # 10 тысяч USDT
                liquidity_bonus = Decimal("2")

            price_factor = max(
                Decimal("0.3"), Decimal("1.5") - (price / Decimal("50"))
            )  # Меньше влияния цены

            # 🔴 ФИКС: МЕНЕЕ СТРОГИЕ ШТРАФЫ
            volatility_penalty = Decimal("0")
            if abs(change) > Decimal("100"):  # Только при очень высокой волатильности
                volatility_penalty = (abs(change) - Decimal("100")) * Decimal("0.05")

            commission_cost = self.commission * Decimal(
                "50"
            )  # Уменьшил влияние комиссии

            final_score = (
                (price_score + volume_score) * price_factor
                - volatility_penalty
                - commission_cost
                + liquidity_bonus
            )

            return final_score

        except Exception as e:
            logger.error(f"❌ Ошибка расчета скора: {e}")
            return Decimal("0")

    def categorize_asset(self, symbol, price, volume, change_24h):
        """Категоризация актива с учетом ликвидности"""
        category = ""

        if price < Decimal("0.01"):
            category = "micro_cap"
        elif price < Decimal("1"):
            category = "low_cap"
        elif price < Decimal("10"):
            category = "mid_cap"
        else:
            category = "high_cap"

        # Логируем экстремально низкие объемы
        # if volume < Decimal("100000"):  # 100,000 USDT
        #     logger.warning(
        #         f"⚠️ ОЧЕНЬ НИЗКАЯ ЛИКВИДНОСТЬ: {symbol}/USDT - Объем: {volume:.2f} USDT"
        #     )

        return category

    def debug_balance_structure(self, balance):
        """Вспомогательный метод для отладки структуры баланса"""
        logger.info("🔍 ДЕБАГ СТРУКТУРЫ БАЛАНСА:")

        if not balance:
            logger.info("   Баланс пуст")
            return

        # Логируем основные ключи
        logger.info(f"   Основные ключи: {list(balance.keys())}")

        # Смотрим структуру free, used, total
        for key in ["free", "used", "total"]:
            if key in balance:
                currencies = []
                for currency, amount in balance[key].items():
                    if float(amount) > 0.0001:  # Только значительные суммы
                        currencies.append(f"{currency}: {amount}")
                if currencies:
                    logger.info(
                        f"   {key.upper()}: {', '.join(currencies[:10])}"
                    )  # Первые 10

        # Смотрим структуру info если есть
        if "info" in balance:
            logger.info("   INFO структура присутствует")
            if isinstance(balance["info"], dict) and "result" in balance["info"]:
                result = balance["info"]["result"]
                if isinstance(result, dict) and "list" in result:
                    assets = result["list"]
                    logger.info(f"   Найдено активов в info: {len(assets)}")
                    for asset in assets[:5]:  # Первые 5 активов
                        if "coin" in asset and "free" in asset:
                            if float(asset["free"]) > 0.0001:
                                logger.info(f"      {asset['coin']}: {asset['free']}")

    def debug_balance_info(self):
        """Упрощенная диагностика баланса (вызывать только при проблемах)"""
        try:
            logger.info("🔍 ДИАГНОСТИКА БАЛАНСА:")
            balance = self.exchange.fetch_balance()

            # Только ключевая информация
            if "free" in balance:
                logger.info("🆓 СВОБОДНЫЕ СРЕДСТВА:")
                for currency, amount in balance["free"].items():
                    if float(amount) > 0.01:  # Только значительные суммы
                        logger.info(f"   {currency}: {amount}")

        except Exception as e:
            logger.error(f"❌ Ошибка диагностики баланса: {e}")

    def get_usdt_balance(self):
        """Надежное получение баланса USDT с улучшенной обработкой ошибок"""
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                balance = self.exchange.fetch_balance(params={"type": "spot"})
                usdt_balance = Decimal("0")

                # Метод 1: прямой доступ к свободным USDT
                if "USDT" in balance and isinstance(balance["USDT"], dict):
                    usdt_balance = Decimal(str(balance["USDT"].get("free", 0)))
                    logger.debug(f"💰 Баланс USDT (free): {usdt_balance}")
                    return usdt_balance

                # Метод 2: через free
                if "free" in balance and "USDT" in balance["free"]:
                    usdt_balance = Decimal(str(balance["free"]["USDT"]))
                    logger.debug(f"💰 Баланс USDT (free alt): {usdt_balance}")
                    return usdt_balance

                # Метод 3: через total
                if "total" in balance and "USDT" in balance["total"]:
                    usdt_balance = Decimal(str(balance["total"]["USDT"]))
                    logger.debug(f"💰 Баланс USDT (total): {usdt_balance}")
                    return usdt_balance

                # Метод 4: через info (Bybit specific)
                if "info" in balance and isinstance(balance["info"], dict):
                    assets = balance["info"].get("result", {}).get("list", [])
                    for asset in assets:
                        if asset.get("coin") == "USDT":
                            usdt_balance = Decimal(str(asset.get("free", 0)))
                            logger.debug(f"💰 Баланс USDT (info): {usdt_balance}")
                            return usdt_balance

                logger.warning(
                    f"⚠️ Баланс USDT не найден в структуре баланса (попытка {attempt + 1})"
                )

            except Exception as e:
                logger.warning(
                    f"⚠️ Ошибка получения баланса (попытка {attempt + 1}): {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue

            break

        # КРИТИЧЕСКАЯ ОШИБКА - лучше остановить бота чем торговать с неизвестным балансом
        logger.error(
            "🚨 КРИТИЧЕСКАЯ ОШИБКА: Не удалось получить баланс USDT после всех попыток"
        )
        raise Exception(
            "Не удалось получить баланс USDT. Проверьте подключение к бирже."
        )

    def get_current_portfolio(self):
        """Получение текущего портфеля с фильтрацией пыли (<1 USDT)"""
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

                        # 🔴 ФИЛЬТРАЦИЯ ПЫЛИ - позиции стоимостью менее 1 USDT
                        position_value = quantity_dec * current_price_dec
                        if position_value < Decimal("1"):
                            logger.info(
                                f"💨 ИСКЛЮЧАЕМ ПЫЛЬ ИЗ ПОРТФЕЛЯ: {symbol} - {position_value:.4f} USDT"
                            )
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
        """Улучшенная синхронизация портфеля с закрытием несуществующих позиций"""
        try:
            logger.info("🔄 ЗАПУСК УЛУЧШЕННОЙ СИНХРОНИЗАЦИИ ПОРТФЕЛЯ")
            if hasattr(self, "debug_balance") and self.debug_balance:
                balance = self.exchange.fetch_balance(params={"type": "spot"})
                self.debug_balance_structure(balance)
            else:
                balance = self.exchange.fetch_balance(params={"type": "spot"})

            added_count = 0
            updated_count = 0
            closed_count = 0

            # Читаем ВСЕ активные позиции из БД
            db_active = {}
            if self.db_conn:
                with self.db_conn.cursor() as cur:
                    cur.execute("""
                        SELECT symbol, quantity, entry_price
                        FROM portfolio WHERE status = 'active'
                    """)
                    for row in cur.fetchall():
                        symbol, quantity, entry_price = row
                        db_active[symbol] = {
                            "quantity": Decimal(str(quantity)),
                            "entry_price": Decimal(str(entry_price)),
                        }

            logger.info(f"📊 Начальный портфель из БД: {len(db_active)} позиций")

            # 🔴 СОБИРАЕМ СИМВОЛЫ, КОТОРЫЕ ЕСТЬ НА БИРЖЕ
            exchange_symbols = set()
            processed_currencies = set()

            for currency, data in balance.items():
                # Пропускаем служебные поля
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
                    free_balance = Decimal(str(data.get("free", 0)))
                    # Пропускаем нулевые балансы
                    if free_balance <= Decimal("0.0001"):
                        continue

                    # Формируем символ для пары
                    symbol = f"{currency}/USDT"
                    bybit_symbol = symbol.replace("/", "")
                    processed_currencies.add(currency)

                    try:
                        market = self.exchange.market(bybit_symbol)
                        logger.debug(f"✅ Найдена пара: {bybit_symbol}")
                    except Exception as e:
                        logger.debug(f"⚠️ Пара не найдена: {bybit_symbol} - {e}")
                        continue

                    try:
                        ticker = self.exchange.fetch_ticker(bybit_symbol)
                        current_price = (
                            Decimal(str(ticker["last"]))
                            if ticker.get("last")
                            else Decimal("0")
                        )
                        if current_price <= 0:
                            logger.debug(f"⚠️ Нулевая цена для {symbol}")
                            continue
                    except Exception as e:
                        logger.warning(f"⚠️ Не удалось получить цену для {symbol}: {e}")
                        continue

                    # Рассчитываем стоимость позиции
                    position_value = free_balance * current_price

                    # 🔴 ФИЛЬТР ПЫЛИ - позиции стоимостью менее 1 USDT
                    if position_value < Decimal("1"):
                        logger.debug(
                            f"💨 Пропускаем пыль: {symbol} ({position_value:.4f} USDT)"
                        )
                        # Если такая позиция есть в БД, закрываем её
                        if symbol in db_active:
                            if self.db_conn:
                                with self.db_conn.cursor() as cur:
                                    cur.execute(
                                        """
                                        UPDATE portfolio
                                        SET status = 'closed', exit_time = NOW()
                                        WHERE symbol = %s AND status = 'active'
                                        """,
                                        (symbol,),
                                    )
                                closed_count += 1
                                logger.info(
                                    f"🔒 Закрыта позиция-пыль: {symbol} ({position_value:.4f} USDT)"
                                )
                        continue

                    exchange_symbols.add(symbol)

                    # === ЛОГИКА СИНХРОНИЗАЦИИ ===
                    if symbol in db_active:
                        # ОБНОВЛЯЕМ СУЩЕСТВУЮЩУЮ ПОЗИЦИЮ
                        with self.db_conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE portfolio
                                SET quantity = %s, current_price = %s
                                WHERE symbol = %s AND status = 'active'
                                """,
                                (float(free_balance), float(current_price), symbol),
                            )
                        if cur.rowcount > 0:
                            updated_count += 1
                            logger.info(
                                f"🔄 ОБНОВЛЕНА: {symbol} | {free_balance} @ {current_price} (стоимость: {position_value:.2f} USDT)"
                            )
                    else:
                        # ДОБАВЛЯЕМ НОВУЮ ПОЗИЦИЮ
                        with self.db_conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO portfolio
                                (symbol, quantity, entry_price, current_price, status)
                                VALUES (%s, %s, %s, %s, 'active')
                                ON CONFLICT (symbol) WHERE status = 'active'
                                DO UPDATE SET
                                    quantity = EXCLUDED.quantity,
                                    entry_price = EXCLUDED.entry_price,
                                    current_price = EXCLUDED.current_price
                                """,
                                (
                                    symbol,
                                    float(free_balance),
                                    float(
                                        current_price
                                    ),  # Для новой позиции entry_price = текущая цена
                                    float(current_price),
                                ),
                            )
                        added_count += 1
                        logger.info(
                            f"✅ ДОБАВЛЕНА: {symbol} | {free_balance} @ {current_price} (стоимость: {position_value:.2f} USDT)"
                        )

            # 🔴 ЗАКРЫВАЕМ ПОЗИЦИИ, КОТОРЫХ НЕТ НА БИРЖЕ ИЛИ КОТОРЫЕ СТАЛИ ПЫЛЬЮ
            if self.db_conn:
                with self.db_conn.cursor() as cur:
                    for symbol in list(db_active.keys()):
                        if symbol not in exchange_symbols:
                            # Проверяем, может быть эту валюту мы просто не обработали из-за формата
                            base_currency = symbol.replace("/USDT", "")
                            if base_currency in processed_currencies:
                                # Если валюта была обработана, но символ не совпал, пропускаем
                                continue

                            cur.execute(
                                """
                                UPDATE portfolio
                                SET status = 'closed', exit_time = NOW()
                                WHERE symbol = %s AND status = 'active'
                                """,
                                (symbol,),
                            )
                            if cur.rowcount > 0:
                                closed_count += 1
                                logger.info(
                                    f"🔒 Закрыта позиция, отсутствующая на бирже: {symbol}"
                                )

            self.db_conn.commit()

            # 🔴 ДОПОЛНИТЕЛЬНАЯ СТАТИСТИКА
            logger.info("📊 СТАТИСТИКА СИНХРОНИЗАЦИИ:")
            logger.info(f"   Обработано валют с балансом: {len(processed_currencies)}")
            logger.info(f"   Торговых пар найдено: {len(exchange_symbols)}")
            logger.info(f"   Добавлено позиций: {added_count}")
            logger.info(f"   Обновлено позиций: {updated_count}")
            logger.info(f"   Закрыто позиций: {closed_count}")

            # Получаем итоговый портфель
            final_portfolio = self.get_current_portfolio()
            logger.info(f"📦 ФИНАЛЬНЫЙ ПОРТФЕЛЬ: {len(final_portfolio)} позиций")

            # Логируем итоговый баланс USDT
            try:
                usdt_balance = self.get_usdt_balance()
                logger.info(f"💰 ИТОГОВЫЙ БАЛАНС USDT: {usdt_balance}")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось получить итоговый баланс: {e}")

            return True

        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации: {e}")
            if self.db_conn:
                self.db_conn.rollback()
            return False

    def update_portfolio_prices(self, tickers):
        """Обновление текущих цен в портфеле"""
        try:
            if not self.db_conn:
                return

            updated_count = 0
            with self.db_conn.cursor() as cur:
                for symbol in tickers:
                    if symbol in tickers:
                        current_price = tickers[symbol]["price"]
                        cur.execute(
                            """
                            UPDATE portfolio
                            SET current_price = %s
                            WHERE symbol = %s AND status = 'active'
                            """,
                            (float(current_price), symbol),
                        )
                        if cur.rowcount > 0:
                            updated_count += 1

                self.db_conn.commit()

            return updated_count > 0

        except Exception as e:
            logger.error(f"❌ Ошибка обновления цен портфеля: {e}")
            return False

    def analyze_portfolio_diversification(self, portfolio, tickers):
        """Анализ диверсификации портфеля"""
        categories_count = {
            "micro_cap": 0,
            "low_cap": 0,
            "mid_cap": 0,
            "high_cap": 0,
            "meme": 0,
            "defi": 0,
            "ai": 0,
        }

        for symbol in portfolio:
            if symbol in tickers:
                category = tickers[symbol].get("category", "unknown")
                categories_count[category] = categories_count.get(category, 0) + 1

        logger.info("🎯 ДИВЕРСИФИКАЦИЯ ПОРТФЕЛЯ:")
        for category, count in categories_count.items():
            if count > 0:
                logger.info(f"   {category}: {count} позиций")

        return categories_count

    def find_diversified_opportunities(self, tickers, portfolio, current_categories):
        """Поиск возможностей с учетом диверсификации"""
        opportunities = []

        for symbol, data in tickers.items():
            if symbol not in portfolio:
                category = data.get("category", "unknown")
                score = data["score"]
                price = data["price"]

                # Бонус за диверсификацию
                diversification_bonus = Decimal("0")
                if current_categories.get(category, 0) == 0:
                    diversification_bonus = Decimal("10")
                elif current_categories.get(category, 0) <= 1:
                    diversification_bonus = Decimal("5")

                final_score = score + diversification_bonus
                opportunities.append((symbol, final_score, price, category))

        # Сортировка по скору
        opportunities.sort(key=lambda x: x[1], reverse=True)
        return opportunities[:5]

    def cleanup_dust_positions(self):
        """Очистка существующих позиций-пыли из БД"""
        try:
            if not self.db_conn:
                return
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol, quantity, current_price
                    FROM portfolio
                    WHERE status = 'active'
                """)
                dust_positions = []
                for row in cur.fetchall():
                    symbol, quantity, current_price = row
                    position_value = Decimal(str(quantity)) * Decimal(
                        str(current_price)
                    )
                    if position_value < Decimal("1"):
                        dust_positions.append(symbol)
                # Помечаем пыль как закрытую
                for symbol in dust_positions:
                    cur.execute(
                        "UPDATE portfolio SET status = 'closed' WHERE symbol = %s",
                        (symbol,),
                    )
                self.db_conn.commit()
                logger.info(f"🧹 Очищено позиций-пыли: {len(dust_positions)}")
        except Exception as e:
            logger.error(f"❌ Ошибка очистки пыли: {e}")

    def check_stop_conditions(self, portfolio, tickers):
        """Проверка стоп-условий с исправленным трейлинг-стопом"""
        positions_to_sell = []

        for symbol, position in portfolio.items():
            # Получаем текущую цену
            if symbol in tickers:
                current_price = tickers[symbol]["price"]
            else:
                try:
                    bybit_symbol = symbol.replace("/", "")
                    ticker = self.exchange.fetch_ticker(bybit_symbol)
                    current_price = Decimal(str(ticker.get("last", 0)))
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось получить цену для {symbol}: {e}")
                    current_price = position.get("current_price", Decimal("0"))

            entry_price = position["entry_price"]
            entry_time = position["entry_time"]
            quantity = position["quantity"]

            # Пропускаем пыль
            position_value = quantity * current_price
            if position_value < Decimal("1"):
                logger.info(f"💨 Пропускаем пыль в стоп-проверке: {symbol}")
                continue

            pnl_ratio = current_price / entry_price

            # 1. СТОП-ЛОСС
            if pnl_ratio <= self.stop_loss:
                positions_to_sell.append(
                    (symbol, position, current_price, f"СТОП-ЛОСС ({pnl_ratio:.4f})")
                )
                # Удаляем из трейлинг-стопа
                if symbol in self.trailing_stop_max_prices:
                    del self.trailing_stop_max_prices[symbol]
                continue

            # 2. ТЕЙК-ПРОФИТ
            if pnl_ratio >= self.take_profit:
                positions_to_sell.append(
                    (symbol, position, current_price, f"ТЕЙК-ПРОФИТ ({pnl_ratio:.4f})")
                )
                # Удаляем из трейлинг-стопа
                if symbol in self.trailing_stop_max_prices:
                    del self.trailing_stop_max_prices[symbol]
                continue

            # 3. ТРЕЙЛИНГ-СТОП (ИСПРАВЛЕННАЯ ЛОГИКА)
            if pnl_ratio > Decimal("1.01"):  # Активируем только при профите > 1%
                # Инициализируем максимальную цену если нужно
                if symbol not in self.trailing_stop_max_prices:
                    self.trailing_stop_max_prices[symbol] = current_price
                else:
                    # Обновляем максимальную цену
                    if current_price > self.trailing_stop_max_prices[symbol]:
                        self.trailing_stop_max_prices[symbol] = current_price
                        logger.debug(
                            f"📈 Обновлен трейлинг-стоп для {symbol}: {current_price}"
                        )

                # Проверяем срабатывание трейлинг-стопа
                trailing_trigger_price = (
                    self.trailing_stop_max_prices[symbol] * self.trailing_stop
                )
                if current_price <= trailing_trigger_price:
                    positions_to_sell.append(
                        (
                            symbol,
                            position,
                            current_price,
                            f"ТРЕЙЛИНГ-СТОП (макс: {self.trailing_stop_max_prices[symbol]:.6f}, текущая: {current_price:.6f})",
                        )
                    )
                    # Удаляем из трейлинг-стопа
                    del self.trailing_stop_max_prices[symbol]
                    continue

            # 4. ВРЕМЯ ИСТЕКЛО
            hold_time = datetime.now() - entry_time
            if hold_time > timedelta(hours=self.max_hold_hours):
                positions_to_sell.append(
                    (symbol, position, current_price, f"ВРЕМЯ ИСТЕКЛО ({hold_time})")
                )
                # Удаляем из трейлинг-стопа
                if symbol in self.trailing_stop_max_prices:
                    del self.trailing_stop_max_prices[symbol]
                continue

        return positions_to_sell

    def execute_buy_order(self, symbol, amount_usdt, price):
        """Покупка с таймаутом и улучшенной обработкой ошибок"""
        max_wait_time = 30  # секунд максимум ожидания
        start_time = time.time()

        try:
            logger.info(f"🛒 ДЕТАЛИ ПОКУПКИ {symbol}:")
            logger.info(f"   Запрошенная сумма: {amount_usdt} USDT")
            logger.info(f"   Ожидаемая цена: {price}")

            # ПРОВЕРКА БАЛАНСА
            available_balance = self.get_usdt_balance()
            logger.info(f"   Доступный баланс: {available_balance} USDT")
            if available_balance < amount_usdt:
                logger.error(
                    f"❌ НЕДОСТАТОЧНО СРЕДСТВ: {available_balance} < {amount_usdt}"
                )
                return False

            # ФОРМАТ СИМВОЛА
            bybit_symbol = symbol.replace("/", "")
            logger.info(f"   Bybit символ: {bybit_symbol}")

            # ПРОВЕРКА ПАРЫ
            try:
                market = self.exchange.market(bybit_symbol)
                logger.info(f"   Пара найдена: {market['id']}")
            except Exception as e:
                logger.error(f"❌ ПАРА НЕ НАЙДЕНА: {bybit_symbol} - {e}")
                return False

            # РАСЧЁТ КОЛИЧЕСТВА И ПРОВЕРКА МИНИМУМОВ
            quantity = amount_usdt / price

            try:
                precision_quantity = Decimal(
                    str(
                        self.exchange.amount_to_precision(bybit_symbol, float(quantity))
                    )
                )
                logger.info(f"   Количество с точностью: {precision_quantity}")

                min_amount = market["limits"]["amount"]["min"]
                min_cost = (
                    market["limits"]["cost"]["min"]
                    if market["limits"]["cost"]["min"]
                    else 0
                )

                logger.info(f"   Минимальное количество: {min_amount}")
                logger.info(f"   Минимальная стоимость: {min_cost}")

                if float(precision_quantity) < min_amount:
                    logger.error(
                        f"❌ Количество < min: {precision_quantity} < {min_amount}"
                    )
                    return False

                order_cost = float(precision_quantity) * float(price)
                if min_cost > 0 and order_cost < min_cost:
                    logger.error(f"❌ Стоимость < min: {order_cost} < {min_cost}")
                    return False

            except Exception as e:
                logger.error(f"❌ Ошибка расчёта: {e}")
                return False

            # СОЗДАНИЕ ОРДЕРА
            logger.info(f"🎯 ВЫПОЛНЕНИЕ ПОКУПКИ: {bybit_symbol}")
            logger.info(
                f"   Тип: market, Сторона: buy, Количество: {float(precision_quantity)}"
            )

            try:
                order = self.exchange.create_order(
                    symbol=bybit_symbol,
                    type="market",
                    side="buy",
                    amount=float(precision_quantity),
                )
                order_id = order.get("id")
                logger.info(f"✅ ОРДЕР СОЗДАН: {order_id}")

                # === RETRY С ТАЙМАУТОМ ===
                max_retries = 6
                retry_delay = 1.5
                order_executed = False
                actual_price = price
                filled_amount = Decimal("0")

                for attempt in range(1, max_retries + 1):
                    # ПРОВЕРКА ТАЙМАУТА
                    if time.time() - start_time > max_wait_time:
                        logger.error("⏰ ТАЙМАУТ подтверждения ордера")
                        # Пытаемся отменить ордер
                        try:
                            self.exchange.cancel_order(order_id, bybit_symbol)
                            logger.info("🛑 Ордер отменен по таймауту")
                        except Exception as cancel_error:
                            logger.warning(
                                f"⚠️ Не удалось отменить ордер: {cancel_error}"
                            )
                        return False

                    logger.debug(f"🕒 Попытка подтверждения #{attempt}/{max_retries}")
                    time.sleep(retry_delay)

                    # 1. ПРЯМОЙ ЗАПРОС fetch_order
                    try:
                        order_info = self.exchange.fetch_order(order_id, bybit_symbol)
                        if (
                            order_info.get("status") == "closed"
                            and float(order_info.get("filled", 0)) > 0
                        ):
                            order_executed = True
                            actual_price = Decimal(
                                str(
                                    order_info.get("average")
                                    or order_info.get("price")
                                    or price
                                )
                            )
                            filled_amount = Decimal(str(order_info.get("filled", 0)))
                            logger.info(f"✅ УСПЕХ: fetch_order (попытка {attempt})")
                            logger.info(f"   Исполнено: {filled_amount}")
                            logger.info(f"   Средняя цена: {actual_price}")
                            break
                    except Exception as e:
                        logger.debug(f"📡 fetch_order: {e}")

                    # 2. ПОИСК В ЗАКРЫТЫХ ОРДЕРАХ
                    try:
                        since = int((time.time() - 1800) * 1000)  # 30 минут
                        closed_orders = self.exchange.fetch_closed_orders(
                            bybit_symbol, since=since, limit=100
                        )
                        for co in closed_orders:
                            if str(co.get("id")) == str(order_id):
                                order_executed = True
                                actual_price = Decimal(
                                    str(co.get("average") or co.get("price") or price)
                                )
                                filled_amount = Decimal(str(co.get("filled", 0)))
                                logger.info(
                                    f"✅ УСПЕХ: closed_orders (попытка {attempt})"
                                )
                                logger.info(f"   Исполнено: {filled_amount}")
                                logger.info(f"   Средняя цена: {actual_price}")
                                break
                        if order_executed:
                            break
                    except Exception as e:
                        logger.debug(f"📡 closed_orders: {e}")

                # === ПРОВЕРКА РЕЗУЛЬТАТА ===
                if not order_executed:
                    logger.error(
                        f"❌ ОРДЕР {order_id} НЕ ПОДТВЕРЖДЁН ПОСЛЕ {max_retries} ПОПЫТОК"
                    )
                    logger.error("🚨 ПОКУПКА НЕ СОХРАНЕНА — ПРОВЕРЬТЕ ВРУЧНУЮ НА BYBIT")
                    return False

                # === СОХРАНЕНИЕ В БД ===
                final_quantity = (
                    filled_amount
                    if filled_amount > Decimal("0")
                    else precision_quantity
                )
                final_price = actual_price

                if self.db_conn:
                    with self.db_conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO portfolio
                            (symbol, quantity, entry_price, current_price, status)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                symbol,
                                float(final_quantity),
                                float(final_price),
                                float(final_price),
                                "active",
                            ),
                        )
                    self.db_conn.commit()

                logger.info(
                    f"💾 ПОЗИЦИЯ СОХРАНЕНА: {symbol} | {final_quantity} @ {final_price}"
                )

                # ОБНОВЛЕНИЕ БАЛАНСА
                new_balance = self.get_usdt_balance()
                logger.info(f"💰 НОВЫЙ БАЛАНС: {new_balance} USDT")
                return True

            except Exception as order_error:
                logger.error(f"❌ ОШИБКА СОЗДАНИЯ ОРДЕРА {bybit_symbol}: {order_error}")
                if "retCode" in str(order_error):
                    logger.error(f"📟 Код ошибки Bybit: {order_error}")
                return False

        except Exception as e:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПОКУПКИ {symbol}: {e}")
            return False

    def execute_sell_order(self, symbol, quantity, price):
        """Продажа с проверкой реального баланса и синхронизацией"""
        try:
            logger.info(f"💰 ДЕТАЛИ ПРОДАЖИ {symbol}:")
            logger.info(f"   Количество из базы: {quantity}")
            logger.info(f"   Ожидаемая цена: {price}")

            # ПРОВЕРКА ФОРМАТА
            if ":" in symbol:
                symbol = symbol.replace(":", "/")
                logger.info(f"   Исправленный символ: {symbol}")

            # 🔴 ВАЖНО: Сначала синхронизируем портфель
            logger.info("🔄 Проверка реального баланса перед продажей...")
            self.sync_portfolio_with_exchange()

            # Получаем актуальный портфель
            portfolio = self.get_current_portfolio()
            if symbol not in portfolio:
                logger.error(f"❌ ПОЗИЦИЯ НЕ НАЙДЕНА В ПОРТФЕЛЕ: {symbol}")
                return Decimal("0")

            # 🔴 ПРОВЕРЯЕМ РЕАЛЬНЫЙ БАЛАНС НА БИРЖЕ
            bybit_symbol = symbol.replace("/", "")
            base_currency = bybit_symbol.replace("USDT", "")

            try:
                balance = self.exchange.fetch_balance(params={"type": "spot"})
                real_balance = Decimal("0")

                # Ищем реальный баланс монеты
                if base_currency in balance and isinstance(
                    balance[base_currency], dict
                ):
                    real_balance = Decimal(str(balance[base_currency].get("free", 0)))
                elif "free" in balance and base_currency in balance["free"]:
                    real_balance = Decimal(str(balance["free"][base_currency]))

                logger.info(f"🔍 РЕАЛЬНЫЙ БАЛАНС {base_currency}: {real_balance}")
                logger.info(f"🔍 БАЛАНС ИЗ БАЗЫ: {quantity}")

                # Если реальный баланс меньше, используем реальный
                if real_balance < quantity:
                    logger.warning(
                        f"⚠️ Реальный баланс меньше базы: {real_balance} < {quantity}"
                    )
                    if real_balance > Decimal("0"):
                        quantity = real_balance
                        logger.info(f"🔄 Используем реальный баланс: {quantity}")
                    else:
                        logger.error(f"❌ Нулевой реальный баланс для {symbol}")
                        # Закрываем позицию в БД
                        if self.db_conn:
                            with self.db_conn.cursor() as cur:
                                cur.execute(
                                    """
                                    UPDATE portfolio
                                    SET status = 'closed', exit_time = NOW()
                                    WHERE symbol = %s AND status = 'active'
                                    """,
                                    (symbol,),
                                )
                            self.db_conn.commit()
                            logger.info(
                                f"🔒 Позиция закрыта в БД: {symbol} (нулевой баланс)"
                            )
                        return Decimal("0")

            except Exception as e:
                logger.error(f"❌ Ошибка проверки реального баланса: {e}")
                # Продолжаем с количеством из базы, но логируем предупреждение

            # ФОРМАТ ДЛЯ BYBIT
            logger.info(f"   Bybit символ: {bybit_symbol}")

            # ПРОВЕРКА ПАРЫ
            try:
                market = self.exchange.market(bybit_symbol)
                logger.info(f"   Пара найдена: {market['id']}")
            except Exception as e:
                logger.error(f"❌ ПАРА НЕ НАЙДЕНА: {e}")
                return Decimal("0")

            # ТОЧНОЕ КОЛИЧЕСТВО
            try:
                precision_quantity = Decimal(
                    str(
                        self.exchange.amount_to_precision(bybit_symbol, float(quantity))
                    )
                )
                logger.info(f"   Количество с точностью: {precision_quantity}")

                # 🔴 ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: если количество стало нулевым после точности
                if precision_quantity <= Decimal("0"):
                    logger.error(
                        f"❌ Количество стало нулевым после точности: {precision_quantity}"
                    )
                    # Закрываем позицию в БД
                    if self.db_conn:
                        with self.db_conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE portfolio
                                SET status = 'closed', exit_time = NOW()
                                WHERE symbol = %s AND status = 'active'
                                """,
                                (symbol,),
                            )
                        self.db_conn.commit()
                    return Decimal("0")

            except Exception as e:
                logger.error(f"❌ Ошибка точности: {e}")
                return Decimal("0")

            # 🔴 ПРОВЕРКА МИНИМАЛЬНОГО КОЛИЧЕСТВА
            try:
                min_amount = market["limits"]["amount"]["min"]
                if float(precision_quantity) < min_amount:
                    logger.error(
                        f"❌ Количество меньше минимального: {precision_quantity} < {min_amount}"
                    )
                    # Закрываем позицию в БД как пыль
                    if self.db_conn:
                        with self.db_conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE portfolio
                                SET status = 'closed', exit_time = NOW()
                                WHERE symbol = %s AND status = 'active'
                                """,
                                (symbol,),
                            )
                        self.db_conn.commit()
                        logger.info(f"💨 Позиция закрыта как пыль: {symbol}")
                    return Decimal("0")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось проверить минимальное количество: {e}")

            # СОЗДАНИЕ ОРДЕРА
            logger.info(f"🎯 ВЫПОЛНЕНИЕ ПРОДАЖИ: {bybit_symbol}")
            logger.info(
                f"   Тип: market, Сторона: sell, Количество: {float(precision_quantity)}"
            )

            try:
                order = self.exchange.create_order(
                    symbol=bybit_symbol,
                    type="market",
                    side="sell",
                    amount=float(precision_quantity),
                )
                order_id = order.get("id")
                logger.info(f"✅ ОРДЕР СОЗДАН: {order_id}")

                # === RETRY С ТАЙМАУТОМ ===
                max_retries = 6
                retry_delay = 1.5
                order_executed = False
                actual_price = price
                filled_amount = Decimal("0")
                proceeds = Decimal("0")

                for attempt in range(1, max_retries + 1):
                    if time.time() - start_time > max_wait_time:
                        logger.error("⏰ ТАЙМАУТ подтверждения ордера")
                        try:
                            self.exchange.cancel_order(order_id, bybit_symbol)
                            logger.info("🛑 Ордер отменен по таймауту")
                        except Exception as cancel_error:
                            logger.warning(
                                f"⚠️ Не удалось отменить ордер: {cancel_error}"
                            )
                        return Decimal("0")

                    logger.debug(f"🕒 Попытка подтверждения #{attempt}/{max_retries}")
                    time.sleep(retry_delay)

                    # 1. ПРЯМОЙ ЗАПРОС fetch_order
                    try:
                        order_info = self.exchange.fetch_order(order_id, bybit_symbol)
                        if (
                            order_info.get("status") == "closed"
                            and float(order_info.get("filled", 0)) > 0
                        ):
                            order_executed = True
                            actual_price = Decimal(
                                str(
                                    order_info.get("average")
                                    or order_info.get("price")
                                    or price
                                )
                            )
                            filled_amount = Decimal(str(order_info.get("filled", 0)))
                            proceeds = filled_amount * actual_price
                            logger.info(f"✅ УСПЕХ: fetch_order (попытка {attempt})")
                            logger.info(f"   Исполнено: {filled_amount}")
                            logger.info(f"   Средняя цена: {actual_price}")
                            logger.info(f"   Выручка: {proceeds:.2f} USDT")
                            break
                    except Exception as e:
                        logger.debug(f"📡 fetch_order: {e}")

                    # 2. ПОИСК В ЗАКРЫТЫХ ОРДЕРАХ
                    try:
                        since = int((time.time() - 1800) * 1000)
                        closed_orders = self.exchange.fetch_closed_orders(
                            bybit_symbol, since=since, limit=100
                        )
                        for co in closed_orders:
                            if str(co.get("id")) == str(order_id):
                                order_executed = True
                                actual_price = Decimal(
                                    str(co.get("average") or co.get("price") or price)
                                )
                                filled_amount = Decimal(str(co.get("filled", 0)))
                                proceeds = filled_amount * actual_price
                                logger.info(
                                    f"✅ УСПЕХ: closed_orders (попытка {attempt})"
                                )
                                logger.info(f"   Исполнено: {filled_amount}")
                                logger.info(f"   Средняя цена: {actual_price}")
                                logger.info(f"   Выручка: {proceeds:.2f} USDT")
                                break
                        if order_executed:
                            break
                    except Exception as e:
                        logger.debug(f"📡 closed_orders: {e}")

                if not order_executed:
                    logger.error(
                        f"❌ ОРДЕР {order_id} НЕ ПОДТВЕРЖДЁН ПОСЛЕ {max_retries} ПОПЫТОК"
                    )
                    return Decimal("0")

                # === ЗАКРЫТИЕ В БД ===
                if self.db_conn:
                    with self.db_conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE portfolio
                            SET status = 'closed',
                                exit_price = %s,
                                exit_time = NOW()
                            WHERE symbol = %s AND status = 'active'
                            """,
                            (float(actual_price), symbol),
                        )
                    self.db_conn.commit()

                logger.info(f"💾 ПОЗИЦИЯ ЗАКРЫТА В БД: {symbol}")

                # Удаляем из трейлинг-стопа
                if symbol in self.trailing_stop_max_prices:
                    del self.trailing_stop_max_prices[symbol]

                logger.info(f"✅ ПРОДАЖА УСПЕШНА: +{proceeds:.2f} USDT")
                return proceeds

            except Exception as order_error:
                logger.error(f"❌ ОШИБКА СОЗДАНИЯ ОРДЕРА: {order_error}")
                # 🔴 ЕСЛИ ОШИБКА "Insufficient balance" - ЗАКРЫВАЕМ ПОЗИЦИЮ В БД
                if "Insufficient balance" in str(order_error):
                    logger.error(
                        "🚨 Обнаружена рассинхронизация: позиция в БД, но нет на бирже"
                    )
                    if self.db_conn:
                        with self.db_conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE portfolio
                                SET status = 'closed', exit_time = NOW()
                                WHERE symbol = %s AND status = 'active'
                                """,
                                (symbol,),
                            )
                        self.db_conn.commit()
                        logger.info(f"🔒 Позиция принудительно закрыта в БД: {symbol}")
                return Decimal("0")

        except Exception as e:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРОДАЖИ {symbol}: {e}")
            return Decimal("0")

    def log_enhanced_portfolio_status(self, portfolio, tickers):
        """Логирование статуса портфеля с улучшенным определением категорий"""
        try:
            total_value = Decimal("0")
            total_pnl = Decimal("0")
            category_value = {}

            logger.info("📊 СТАТУС ПОРТФЕЛЯ:")

            if not portfolio:
                logger.info("   💡 Портфель пуст")
                return

            for symbol, position in portfolio.items():
                # Получаем текущую цену и категорию
                current_price = position.get("current_price", Decimal("0"))
                category = "unknown"

                # Пытаемся получить актуальные данные
                try:
                    bybit_symbol = symbol.replace("/", "")
                    ticker = self.exchange.fetch_ticker(bybit_symbol)
                    if ticker.get("last"):
                        current_price = Decimal(str(ticker["last"]))

                    # Определяем категорию по цене
                    if current_price < Decimal("0.01"):
                        category = "micro_cap"
                    elif current_price < Decimal("1"):
                        category = "low_cap"
                    elif current_price < Decimal("10"):
                        category = "mid_cap"
                    else:
                        category = "high_cap"

                except Exception as e:
                    # Если не удалось получить актуальные данные, используем сохраненные
                    pass

                quantity = position["quantity"]
                entry_price = position["entry_price"]

                current_value = quantity * current_price
                total_value += current_value
                category_value[category] = (
                    category_value.get(category, Decimal("0")) + current_value
                )

                pnl = (current_price - entry_price) * quantity
                total_pnl += pnl
                pnl_percent = (
                    ((current_price / entry_price) - Decimal("1")) * Decimal("100")
                    if entry_price > Decimal("0")
                    else Decimal("0")
                )

                # Показываем PnL с цветовой индикацией
                pnl_sign = "🟢" if pnl >= 0 else "🔴"
                logger.info(
                    f"   {symbol} [{category}]: {pnl_sign} PnL: {pnl:.2f} USDT ({pnl_percent:.2f}%)"
                )

            balance = self.get_usdt_balance()
            total_assets = total_value + balance

            logger.info(f"💰 РАСПРЕДЕЛЕНИЕ ПО КАТЕГОРИЯМ:")
            for category, value in category_value.items():
                percent = (
                    (value / total_value * Decimal("100"))
                    if total_value > Decimal("0")
                    else Decimal("0")
                )
                logger.info(f"   {category}: {value:.2f} USDT ({percent:.1f}%)")

            logger.info(f"📈 ИТОГО:")
            logger.info(f"     Баланс USDT: {balance:.2f}")
            logger.info(f"     Стоимость позиций: {total_value:.2f} USDT")
            logger.info(f"     Общие активы: {total_assets:.2f} USDT")

            total_pnl_sign = "🟢" if total_pnl >= 0 else "🔴"
            logger.info(f"     Общий PnL: {total_pnl_sign} {total_pnl:.2f} USDT")

        except Exception as e:
            logger.error(f"❌ Ошибка логирования портфеля: {e}")

    def enhanced_rebalance(self, iteration):
        """Исправленная ребалансировка с упрощенной диагностикой"""
        try:
            if iteration == 1 or iteration % 10 == 0:
                logger.info(f"🔄 Ребалансировка (итерация #{iteration})")

            # 🔴 ФИКС: УПРОЩЕННАЯ СИНХРОНИЗАЦИЯ
            logger.info("🔄 Синхронизация портфеля...")
            self.sync_portfolio_with_exchange()

            # 🔴 ФИКС: БАЛАНС БЕЗ ЛИШНЕЙ ДИАГНОСТИКИ
            available_balance = self.get_usdt_balance()

            # ЗАГРУЗКА ДАННЫХ
            tickers = self.get_cached_tickers()
            current_portfolio = self.get_current_portfolio()

            logger.info(f"💰 БАЛАНС: {available_balance} USDT")
            logger.info(f"💸 РЕЗЕРВ: {self.reserve_cash} USDT")
            logger.info(f"📊 ПОРТФЕЛЬ: {len(current_portfolio)} позиций")
            logger.info(f"🎯 РЫНОК: {len(tickers)} пар")

            # 🔴 ФИКС: ОБНОВЛЕНИЕ ЦЕН В ПОРТФЕЛЕ ИЗ ТИКЕРОВ
            for symbol, position in current_portfolio.items():
                if symbol in tickers:
                    current_price = tickers[symbol]["price"]
                    position_value = position["quantity"] * current_price

                    # Обновляем текущую цену в позиции
                    position["current_price"] = current_price

                    # 🔴 ФИКС: АВТОМАТИЧЕСКОЕ ЗАКРЫТИЕ ПОЗИЦИЙ-ПЫЛИ
                    if position_value < Decimal("1"):
                        logger.info(
                            f"💨 ПОЗИЦИЯ СТАЛА ПЫЛЬЮ: {symbol} - {position_value:.4f} USDT"
                        )
                        if self.db_conn:
                            with self.db_conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE portfolio SET status = 'closed' WHERE symbol = %s AND status = 'active'",
                                    (symbol,),
                                )
                            self.db_conn.commit()
                        # Удаляем из текущего портфеля
                        continue

            # 🔴 ФИКС: ПЕРЕЗАГРУЖАЕМ ПОРТФЕЛЬ ПОСЛЕ ОЧИСТКИ ПЫЛИ
            current_portfolio = self.get_current_portfolio()
            logger.info(f"📊 ОБНОВЛЕННЫЙ ПОРТФЕЛЬ: {len(current_portfolio)} позиций")

            # ОБНОВЛЕНИЕ СТАТУСА ПОРТФЕЛЯ
            current_time = time.time()
            if current_time - self.last_status_log >= self.status_log_interval:
                self.log_enhanced_portfolio_status(current_portfolio, tickers)
                self.last_status_log = current_time

            # 🔴 ФИКС: ПРИНУДИТЕЛЬНАЯ ПРОДАЖА - ИСПОЛЬЗУЕМ ДАННЫЕ ИЗ ТИКЕРОВ
            if len(current_portfolio) > self.max_positions:
                logger.warning(
                    f"🚨 ПРЕВЫШЕН ЛИМИТ ПОЗИЦИЙ: {len(current_portfolio)}/{self.max_positions}"
                )
                logger.info("🔍 Поиск позиций для принудительной продажи...")

                positions_with_pnl = []
                for symbol, position in current_portfolio.items():
                    try:
                        # 🔴 ФИКС: ИСПОЛЬЗУЕМ ЦЕНЫ ИЗ ТИКЕРОВ ВМЕСТО ОТДЕЛЬНЫХ ЗАПРОСОВ
                        if symbol in tickers:
                            current_price = tickers[symbol]["price"]
                        else:
                            # Если нет в тикерах, получаем отдельно
                            bybit_symbol = symbol.replace("/", "")
                            ticker_data = self.exchange.fetch_ticker(bybit_symbol)
                            current_price = (
                                Decimal(str(ticker_data["last"]))
                                if ticker_data.get("last")
                                else position.get("current_price", Decimal("0"))
                            )

                        entry_price = position["entry_price"]
                        if entry_price > Decimal("0"):
                            pnl_ratio = current_price / entry_price
                            positions_with_pnl.append(
                                (symbol, position, current_price, pnl_ratio)
                            )
                            logger.info(
                                f"   {symbol}: цена {current_price}, PnL: {(pnl_ratio - 1) * 100:.2f}%"
                            )

                    except Exception as e:
                        logger.error(f"❌ Ошибка получения цены для {symbol}: {e}")
                        continue

                if positions_with_pnl:
                    # Сортируем по худшему PnL
                    positions_with_pnl.sort(key=lambda x: x[3])

                    positions_to_sell_count = (
                        len(current_portfolio) - self.max_positions
                    )
                    logger.info(f"🔻 Нужно продать {positions_to_sell_count} позиций")

                    sold_count = 0
                    for i in range(
                        min(positions_to_sell_count, len(positions_with_pnl))
                    ):
                        symbol, position, current_price, pnl_ratio = positions_with_pnl[
                            i
                        ]
                        logger.info(
                            f"🔻 Продажа худшей позиции {symbol} (PnL: {(pnl_ratio - 1) * 100:.2f}%)"
                        )

                        sold_amount = self.execute_sell_order(
                            symbol, position["quantity"], current_price
                        )
                        if sold_amount > Decimal("0"):
                            sold_count += 1
                            logger.info(f"   ✅ Продано: {sold_amount:.2f} USDT")
                        else:
                            logger.error(f"   ❌ Ошибка продажи {symbol}")

                    if sold_count > 0:
                        # 🔴 ФИКС: ОБНОВЛЯЕМ ДАННЫЕ ПОСЛЕ ПРОДАЖ
                        available_balance = self.get_usdt_balance()
                        current_portfolio = (
                            self.get_current_portfolio()
                        )  # Перезагружаем портфель
                        logger.info(f"💰 ОБНОВЛЕННЫЙ БАЛАНС: {available_balance} USDT")
                        logger.info(
                            f"📊 ОБНОВЛЕННЫЙ ПОРТФЕЛЬ: {len(current_portfolio)} позиций"
                        )
                    else:
                        logger.error("❌ Не удалось продать ни одной позиции")
                else:
                    logger.error("❌ Не удалось получить данные о позициях для продажи")

            # ПРОДАЖИ ПО СТОП-УСЛОВИЯМ
            positions_to_sell = self.check_stop_conditions(current_portfolio, tickers)
            if positions_to_sell:
                logger.info("🚨 АКТИВНЫЕ СТОП-УСЛОВИЯ:")
                for symbol, position, current_price, reason in positions_to_sell:
                    logger.info(f"   {symbol}: {reason} | Цена: {current_price}")
                    sold_amount = self.execute_sell_order(
                        symbol, position["quantity"], current_price
                    )
                    if sold_amount > Decimal("0"):
                        logger.info(f"   💰 Выручено: {sold_amount:.2f} USDT")

                # 🔴 ФИКС: ОБНОВЛЯЕМ ДАННЫЕ ПОСЛЕ ПРОДАЖ
                available_balance = self.get_usdt_balance()
                current_portfolio = self.get_current_portfolio()

            # ПОКУПКИ - ТОЛЬКО ПРИ ДОСТАТОЧНОМ БАЛАНСЕ И СВОБОДНЫХ СЛОТАХ
            can_trade = available_balance >= self.min_order + self.reserve_cash
            has_free_slots = len(current_portfolio) < self.max_positions

            if can_trade and has_free_slots:
                logger.info("🎯 ДОСТАТОЧНО СРЕДСТВ ДЛЯ ТОРГОВЛИ")

                buy_power = available_balance - self.reserve_cash
                logger.info(f"💪 ДОСТУПНО ДЛЯ ПОКУПОК: {buy_power} USDT")

                # АНАЛИЗ ДИВЕРСИФИКАЦИИ
                current_categories = self.analyze_portfolio_diversification(
                    current_portfolio, tickers
                )

                # ПОИСК ВОЗМОЖНОСТЕЙ
                best_opportunities = self.find_diversified_opportunities(
                    tickers, current_portfolio, current_categories
                )

                bought_count = 0
                for symbol, score, price, category in best_opportunities:
                    if (
                        buy_power >= self.min_order
                        and bought_count < 2
                        and score > Decimal("1")
                    ):
                        amount_to_spend = min(self.position_size, buy_power)

                        if self.execute_buy_order(symbol, amount_to_spend, price):
                            bought_count += 1
                            # 🔴 ФИКС: ОБНОВЛЯЕМ ДАННЫЕ ПОСЛЕ ПОКУПКИ
                            available_balance = self.get_usdt_balance()
                            current_portfolio = self.get_current_portfolio()
                            buy_power = available_balance - self.reserve_cash
                            logger.info(f"🔄 ОСТАТОК ДЛЯ ПОКУПОК: {buy_power} USDT")
                            logger.info(
                                f"📊 ОБНОВЛЕННЫЙ ПОРТФЕЛЬ: {len(current_portfolio)} позиций"
                            )

                if bought_count == 0 and best_opportunities:
                    logger.info("ℹ️ Не куплено позиций после отбора")
            else:
                if not can_trade:
                    logger.info(
                        f"💤 Недостаточно средств для покупок. Баланс: {available_balance}, требуется: {self.min_order + self.reserve_cash}"
                    )
                if not has_free_slots:
                    logger.info(
                        f"📦 Достигнут лимит позиций: {len(current_portfolio)}/{self.max_positions}"
                    )

            return True

        except Exception as e:
            logger.error(f"❌ Ошибка ребалансировки: {e}")
            return False

    def run_optimized(self):
        """Основной цикл с улучшенным мониторингом"""
        logger.info("🚀 Запуск спот-бота Bybit")
        logger.info(f"💼 Капитал: {self.total_capital} USDT")
        logger.info(f"🎯 Цели: TP={self.take_profit} | SL={self.stop_loss}")
        logger.info(f"⏱️  Интервал: 15 секунд")

        iteration = 0
        error_count = 0
        max_errors = 5

        while True:
            try:
                iteration += 1

                if not self.db_conn or self.db_conn.closed:
                    self.db_conn = self.init_db()

                # ВАЖНО: ребалансировка ВСЕГДА пытается выполнить мониторинг
                success = self.enhanced_rebalance(iteration)

                if success:
                    error_count = 0
                    if iteration % 40 == 0:
                        logger.info(f"📈 Прогресс: итерация #{iteration}")
                else:
                    error_count += 1
                    logger.warning(
                        f"⚠️ Ошибка в итерации #{iteration} (ошибок подряд: {error_count})"
                    )
                    if error_count >= max_errors:
                        logger.error("🚨 Превышено количество ошибок, пауза 60 секунд")
                        time.sleep(60)
                        error_count = 0

                time.sleep(15)

            except KeyboardInterrupt:
                logger.info("\n⏹️ Остановка бота...")
                break
            except Exception as e:
                logger.error(f"❌ Критическая ошибка в основном цикле: {e}")
                error_count += 1
                # Даже при критической ошибке продолжаем работу после паузы
                time.sleep(30)

    def force_sell_excess_positions(self, portfolio, tickers):
        """Принудительная продажа избыточных позиций с проверкой баланса"""
        try:
            positions_with_pnl = []
            for symbol, position in portfolio.items():
                try:
                    if symbol in tickers:
                        current_price = tickers[symbol]["price"]
                    else:
                        bybit_symbol = symbol.replace("/", "")
                        ticker_data = self.exchange.fetch_ticker(bybit_symbol)
                        current_price = (
                            Decimal(str(ticker_data["last"]))
                            if ticker_data.get("last")
                            else position.get("current_price", Decimal("0"))
                        )

                    entry_price = position["entry_price"]
                    if entry_price > Decimal("0"):
                        pnl_ratio = current_price / entry_price
                        positions_with_pnl.append(
                            (symbol, position, current_price, pnl_ratio)
                        )
                        logger.info(
                            f"   {symbol}: цена {current_price}, PnL: {(pnl_ratio - 1) * 100:.2f}%"
                        )

                except Exception as e:
                    logger.error(f"❌ Ошибка получения цены для {symbol}: {e}")
                    continue

            if positions_with_pnl:
                positions_to_sell_count = len(portfolio) - self.max_positions
                logger.info(f"🔻 Нужно продать {positions_to_sell_count} позиций")

                # Сортируем по худшему PnL
                positions_with_pnl.sort(key=lambda x: x[3])

                sold_count = 0
                for i in range(min(positions_to_sell_count, len(positions_with_pnl))):
                    symbol, position, current_price, pnl_ratio = positions_with_pnl[i]
                    logger.info(
                        f"🔻 Продажа худшей позиции {symbol} (PnL: {(pnl_ratio - 1) * 100:.2f}%)"
                    )

                    # 🔴 ПЕРЕД ПРОДАЖЕЙ СИНХРОНИЗИРУЕМСЯ
                    self.sync_portfolio_with_exchange()

                    # 🔴 ПРОВЕРЯЕМ, ЧТО ПОЗИЦИЯ ВСЕ ЕЩЕ СУЩЕСТВУЕТ
                    updated_portfolio = self.get_current_portfolio()
                    if symbol not in updated_portfolio:
                        logger.warning(
                            f"⚠️ Позиция {symbol} уже закрыта при синхронизации"
                        )
                        sold_count += 1  # Считаем как проданную
                        continue

                    sold_amount = self.execute_sell_order(
                        symbol, position["quantity"], current_price
                    )
                    if sold_amount > Decimal("0"):
                        sold_count += 1
                        logger.info(f"   ✅ Продано: {sold_amount:.2f} USDT")
                    else:
                        logger.error(f"   ❌ Ошибка продажи {symbol}")

                logger.info(f"🔻 Продано избыточных позиций: {sold_count}")
            else:
                logger.error("❌ Не удалось получить данные о позициях для продажи")

        except Exception as e:
            logger.error(f"❌ Ошибка принудительной продажи: {e}")


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
