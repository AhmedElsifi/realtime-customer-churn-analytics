## Data Requirements

### 1. Customer record attributes

| Attribute              | Field name                | Description                                                             |
| ---------------------- | ------------------------- | ----------------------------------------------------------------------- |
| Customer ID            | `customer_id`             | Unique customer identifier                                              |
| Age                    | `age`                     | Customer's age                                                          |
| Gender                 | `gender`                  | Customer's gender                                                       |
| Location               | `location`                | Customer's geographical location                                        |
| Income level           | `income_level`            | Low / Medium / High                                                     |
| Service duration       | `service_duration_months` | Length of the subscription in months                                    |
| Monthly charges        | `monthly_charges`         | Monthly subscription cost                                               |
| Payment method         | `payment_method`          | Customer's payment method                                               |
| Calls count            | `calls_count`             | Number of calls                                                         |
| Messages count         | `messages_count`          | Number of messages                                                      |
| Data usage             | `data_usage_gb`           | Amount of consumed data (GB)                                            |
| Purchases count        | `purchases_count`         | Purchase history: number of purchases                                   |
| Purchase frequency     | `purchase_frequency`      | How often the customer purchases (e.g. per month)                       |
| Customer service calls | `customer_service_calls`  | Number of interactions with customer support                            |
| Churn status           | `churn_status`            | Yes / No: whether the customer cancelled                                |
| Churn date             | `churn_date`              | Date of cancellation (null if not churned)                              |
| Event timestamp        | `event_timestamp`         | When the record/event was produced (required for time-window analytics) |

### 2. Shared schema (data contract)

All members must use this schema **before writing any code**. Members 1, 2, 3 and 4 depend on identical field names and types.

```json
{
  "customer_id": "C1001",
  "age": 23,
  "gender": "Male",
  "location": "Cairo",
  "income_level": "Medium",
  "service_duration_months": 14,
  "monthly_charges": 150.0,
  "payment_method": "Credit Card",
  "calls_count": 120,
  "messages_count": 300,
  "data_usage_gb": 40.5,
  "purchases_count": 3,
  "purchase_frequency": 1.5,
  "customer_service_calls": 6,
  "churn_status": "Yes",
  "churn_date": "2026-10-01",
  "event_timestamp": "2026-11-18T10:30:00"
}
```

| Field                           | Type              | Notes                                 |
| ------------------------------- | ----------------- | ------------------------------------- |
| `customer_id`                   | string            | Must not be empty                     |
| `age`                           | integer           | Valid range, e.g. 18-80               |
| `gender`                        | string            | Male / Female                         |
| `location`                      | string            | City or governorate                   |
| `income_level`                  | string            | Low / Medium / High                   |
| `service_duration_months`       | integer           | >= 0                                  |
| `monthly_charges`               | double            | > 0                                   |
| `payment_method`                | string            | e.g. Credit Card, Cash, Mobile Wallet |
| `calls_count`, `messages_count` | integer           | >= 0                                  |
| `data_usage_gb`                 | double            | >= 0                                  |
| `purchases_count`               | integer           | >= 0                                  |
| `purchase_frequency`            | double            | Purchases per month                   |
| `customer_service_calls`        | integer           | >= 0                                  |
| `churn_status`                  | string            | Yes / No only                         |
| `churn_date`                    | string (date)     | `null` when `churn_status` is No      |
| `event_timestamp`               | string (ISO 8601) | Used by Spark as event time           |

### 3. Data source

A synthetic Python generator is recommended. Public churn datasets (e.g. Telco) usually lack call counts, income, purchase data and churn dates. The generator should use realistic rules so the analytics reveal real patterns, for example:

- Higher churn probability for high monthly charges.
- Higher churn probability for many customer service calls.
- Higher churn probability for short service duration and low usage.
- Include a small share of intentionally dirty records (missing fields, wrong types) so NiFi and Spark validation have something to catch.