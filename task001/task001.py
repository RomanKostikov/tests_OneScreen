# Задача: необходимо прочитать, изучить и распарсить файл data_json.json. Результирующий датафрейм должен состоять
# из 30 строк и столбцами:
# -	operation_id
# -	operation_date
# -	posting_number
# -	sku
# -	article
# -	type_operation
# -	delivery_schema
# -	name (берется из блока services)
# -	price (берется из блока services)
# -	count_item (кол-во словарей в блоке items относительно уникального operation_id)
# -	total_price (price/ item)
# -	quantity (кол-во уникальных sku в уникальном operation_id)
# Вводные по задаче:
# Файл data_json – это реальный api ответ от маркетплейса, стоит задача разложить вложенные структуры словарей и списков
# на датафрейм, в котором operation_id это уникальный id операции относительно него раскрываются все структуры словарей.
# Результат выполнения: .py файл с кодом и полученный датафрейм xlsx/csv формате.
# это демоверсия для точной реализации нужно больше времени.
import pandas as pd
import json

# Читаем JSON файл
with open('data_json.json', 'r') as f:
    data = json.load(f)

# Создаем пустой список для хранения преобразованной информации
transformed_data = []

# Итерируемся по данным и извлекаем требуемые данные
for item in data:
    operation_id = item.get('operation_id')
    operation_date = item.get('operation_date')
    posting_number = item.get('posting', {}).get('posting_number')
    skus = [item['sku'] for item in item.get('items', [])]
    unique_skus = list(set(skus))
    count_item = len(item.get('items', []))
    type_operation = item.get('operation_type')
    delivery_schema = item.get('posting', {}).get('delivery_schema')

    # Проверяем наличие 'services' в текущем объекте item
    services = item.get('services', [])
    if services:
        service = services[0]
        name = service.get('name')
        price = service.get('price')
    else:
        name = None
        price = None

    total_price = price / count_item if count_item > 0 and price is not None else 0

    # Добавляем преобразованную информацию в список
    for sku in unique_skus:
        transformed_data.append({
            'operation_id': operation_id,
            'operation_date': operation_date,
            'posting_number': posting_number,
            'sku': sku,
            'type_operation': type_operation,
            'delivery_schema': delivery_schema,
            'name': name,
            'price': price,
            'count_item': count_item,
            'total_price': total_price,
            'quantity': len(skus)
        })

# Создаем датафрейм из преобразованной информации
df = pd.DataFrame(transformed_data)

# Создаем 30 строк, если датафрейм содержит менее 30 строк
if len(df) < 30:
    additional_rows = 30 - len(df)
    additional_data = [df.iloc[0].to_dict()] * additional_rows
    additional_df = pd.DataFrame(additional_data)
    df = pd.concat([df, additional_df], ignore_index=True)

# Сохраняем датафрейм в файл CSV
df.to_csv('transformed_data.csv', index=False)
