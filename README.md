# DE-project: Airflow + Spark + S3 + Greenplum (TPC‑H)

## О проекте

Учебно‑практический проект по построению пайплайнов для аналитической платформы:

- **источник данных** - бенчмарк TPC‑H;
- **озеро данных** - S3 (VK Cloud);
- **обработка** - Spark (запуск из Airflow);
- **хранилище** - Greenplum;
- **оркестрация** - Apache Airflow.

Пайплайн реализован в виде набора DAG'ов и Spark‑job’ов, которые читают данные из S3, обрабатывают их и загружают в Greenplum.

---

## Архитектура

### Логическая

<img width="1972" height="1204" alt="image" src="https://github.com/user-attachments/assets/adb8b7de-3059-421b-85ef-a950df80b66c" />

В проекте эмулируется классическая архитектура из DAMA DMBOK:

- **Data Lake (S3)** - хранение сырых и слабо обработанных данных в их исходном формате.
- **Analytical DWH (Greenplum)** - хранение очищенных, структурированных данных, оптимизированных под аналитику.

Основной поток:
**S3 (raw) → Spark (обработка) → S3 (processed) → Greenplum (стейджинг + DWH).**

### Техническая

<img width="1081" height="781" alt="image" src="https://github.com/user-attachments/assets/8d20994b-b98c-4af3-8115-0afcf3e5e4e6" />

Используемые компоненты:

- **S3 (VK Cloud)** - озеро данных;
- **Greenplum** - аналитическое хранилище;
- **Apache Airflow** - оркестратор;
- **Apache Spark** - распределённая обработка (запуск из Airflow, деплой в Kubernetes по `.yaml`);
- **GitLab/GitHub** - хранение исходного кода.

Интеграция Spark ↔ Greenplum:

- Spark читает/пишет в S3;
- Greenplum использует внешние таблицы с хранением в S3.

---

## Структура репозитория

```text
.
├── dags/
│   └── aleksej-plehanov-qsb8985/
│       ├── customers_spark_job.py
│       ├── lineitems_spark_job.py
│       ├── orders_spark_job.py
│       ├── part_spark_job.py
│       ├── suppliers_spark_job.py
│       ├── de-project-aleksej-plehanov-qsb8985.py   # основной DAG
│       ├── spark_customers_app.yaml                 # манифесты Spark‑приложений
│       ├── spark_lineitems_app.yaml
│       ├── spark_orders_app.yaml
│       ├── spark_part_app.yaml
│       ├── spark_suppliers_app.yaml
│       └── ...
├── requirements.txt
└── README.md
```

- `*_spark_job.py` - DAG'и/таски Airflow, которые запускают Spark‑job для конкретной таблицы.
- `spark_*_app.yaml` - Kubernetes‑манифесты для запуска Spark‑приложений.
- `de-project-aleksej-plehanov-qsb8985.py` - главный DAG, связывающий все шаги пайплайна.

---

## Данные (TPC‑H)

В качестве источника данных используется бенчмарк **TPC‑H**.

**Расположение в S3 (VK Cloud):**

```text
/de-project
```

---

## Таблицы

<img width="850" height="802" alt="image" src="https://github.com/user-attachments/assets/9cce89d2-d829-4947-8a75-5bb697d13575" />

| Таблица  | Описание                                   |
|----------|--------------------------------------------|
| customer | информация о заказчиках                   |
| lineitem | информация о позициях в заказах          |
| nation   | информация о нациях/странах              |
| orders   | информация о заказах                     |
| parts    | информация о деталях/запчастях           |
| partsupp | информация о поставках деталей/запчастей |
| region   | информация о регионах                    |
| supplier | информация о поставщиках                 |

---

## Логика пайплайна

### 1. Загрузка сырых данных в Data Lake

- чтение файлов TPC‑H из S3 (`/de-project/raw` или аналогичный путь);
- валидация формата и структуры.

### 2. Обработка в Spark

Для каждой сущности (`customer`, `orders`, `lineitem`, `part`, `supplier`):

- чтение сырых файлов из S3;
- приведение типов и базовая очистка;
- сохранение в S3 в слой **processed** (Parquet или другой колоночный формат).

Технически реализовано через:

- `customers_spark_job.py`, `orders_spark_job.py`, `lineitems_spark_job.py`, `part_spark_job.py`, `suppliers_spark_job.py`;
- соответствующие `spark_*.yaml` ‑ манифесты для запуска в Kubernetes.

### 3. Загрузка в Greenplum

- создание/использование внешних таблиц, указывающих на данные в S3;
- загрузка данных во внутренние таблицы хранилища (стейджинг и основные таблицы).

---
