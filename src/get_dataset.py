import factory
from datetime import datetime, timedelta
import random
import pandas as pd

class SalesRecordFactory(factory.Factory):
    class Meta:
        model = dict
    
    id = factory.Sequence(lambda n: n + 1)
    
    # Аномалия: даты могут быть None или неправильный формат
    sale_date = factory.LazyFunction(
        lambda: None if random.random() < 0.1 else 
        (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d') if random.random() < 0.8 
        else 'invalid_date'
    )
    
    # Аномалия: пробелы, None, пустые строки
    product_name = factory.LazyFunction(
        lambda: None if random.random() < 0.1 else 
        ('  ' + random.choice(['Laptop', 'Phone', 'Tablet', 'Monitor']) + '  ') if random.random() < 0.5
        else random.choice(['Laptop', 'Phone', 'Tablet', 'Monitor', ''])
    )
    
    # Аномалия: отрицательные значения, None, слишком большие
    quantity = factory.LazyFunction(
        lambda: None if random.random() < 0.1 else 
        random.randint(-5, 1000) if random.random() < 0.9
        else 999999
    )
    
    # Аномалия: отрицательные цены, None, 0
    price = factory.LazyFunction(
        lambda: None if random.random() < 0.1 else 
        round(random.uniform(-100, 5000), 2) if random.random() < 0.85
        else 0
    )
    
    # Аномалия: пробелы, разный регистр, None
    category = factory.LazyFunction(
        lambda: None if random.random() < 0.1 else 
        '  ' + random.choice(['Electronics', 'ELECTRONICS', 'electronics', 'Accessories', 'Software']) + '  '
    )
    
    # Аномалия: некорректные email
    customer_email = factory.LazyFunction(
        lambda: None if random.random() < 0.1 else 
        f"customer{random.randint(1, 100)}@example.com" if random.random() < 0.7
        else random.choice(['invalid_email', 'test@', '@example.com', ''])
    )
    
    # Аномалия: дубликаты, None, пробелы
    region = factory.LazyFunction(
        lambda: None if random.random() < 0.1 else 
        '  ' + random.choice(['North', 'South', 'East', 'West', 'north', 'SOUTH']) + '  '
    )
    
    # Аномалия: None, отрицательные, нереалистичные
    discount_percent = factory.LazyFunction(
        lambda: None if random.random() < 0.1 else 
        round(random.uniform(-10, 150), 2)
    )
    
    # Аномалия: некорректный статус
    status = factory.LazyFunction(
        lambda: None if random.random() < 0.1 else 
        random.choice(['completed', 'COMPLETED', 'pending', 'cancelled', 'invalid_status', ''])
    )

    # Аномалия: None, отрицательные рейтинги, >5
    rating = factory.LazyFunction(
        lambda: None if random.random() < 0.15 else 
        round(random.uniform(-1, 7), 1)
    )
    
def get_dataset(num_records=1000):
    records = [SalesRecordFactory() for _ in range(num_records)]
    df = pd.DataFrame(records)
    return df