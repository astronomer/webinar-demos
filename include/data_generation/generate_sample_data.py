import pandas as pd
import random
import uuid
import pandas as pd
import random
import uuid
from datetime import timedelta, datetime
from faker import Faker


def create_sales_reports_csv(filename):
    product_price = {
        "Carrot Plushy": 5.99,
        "ChewChew Train Dog Bed": 34.99,
        "Where is the Ball? - Transparent Edition": 2.99,
        "Stack of Artisinal Homework": 7.99,
        "Post breakfast treats - calory free": 15.99,
    }
    dates = [datetime.today() - timedelta(days=i + 2) for i in range(100)]

    sales_reports = {
        "Date": [],
        "PurchaseID": [],
        "ProductName": [],
        "QuantitySold": [],
        "Revenue": [],
    }

    for i in range(100):
        quantity_sold = random.randint(1, 5)
        product = random.choice(list(product_price.keys()))

        sales_reports["Date"].append(dates[i])
        sales_reports["PurchaseID"].append(uuid.uuid4())
        sales_reports["ProductName"].append(product)
        sales_reports["QuantitySold"].append(quantity_sold)
        sales_reports["Revenue"].append(quantity_sold * product_price[product])

    df = pd.DataFrame(sales_reports)
    df.to_csv(filename, index=False)
    return sales_reports["PurchaseID"]


purchase_ids = create_sales_reports_csv(
    "include/data_generation/data/sales_reports3.csv"
)


def create_customer_feedback_csv(purchase_ids, filename):
    ratings = [1, 2, 3, 4, 5]
    comments = [
        "Woof!",
        "Barktastic!",
        "My dogs can't get enough!",
        "With this my dog can take on any squirrel!",
        "Even after a year of chew, still good!",
    ]

    customer_feedback = {
        "Date": [],
        "CustomerID": [],
        "PurchaseID": [],
        "Rating": [],
        "Comments": [],
    }

    for purchase_id in purchase_ids:
        customer_feedback["Date"].append(pd.Timestamp.now())
        customer_feedback["CustomerID"].append(uuid.uuid4())
        customer_feedback["PurchaseID"].append(purchase_id)
        customer_feedback["Rating"].append(random.choice(ratings))
        customer_feedback["Comments"].append(random.choice(comments))

    df = pd.DataFrame(customer_feedback)
    df.to_csv(filename, index=False)
    return customer_feedback["CustomerID"]


customer_ids = create_customer_feedback_csv(
    purchase_ids, "include/data_generation/data/customer_feedback3.csv"
)


def create_customer_data_csv(customer_ids, filename):
    customer_data = {
        "CustomerID": [],
        "Email": [],
        "Address": [],
        "NumberOfDogs": [],
        "NumPreviousPurchases": [],
    }

    fake = Faker()
    for customer_id in customer_ids:
        customer_data["CustomerID"].append(customer_id)
        customer_data["Email"].append(fake.email())
        customer_data["Address"].append(fake.address().replace("\n", ", "))
        customer_data["NumberOfDogs"].append(random.randint(1, 5))
        customer_data["NumPreviousPurchases"].append(random.randint(1, 20))

    df = pd.DataFrame(customer_data)
    df.to_csv(filename, index=False)
    return customer_data["CustomerID"]


create_customer_data_csv(customer_ids, "include/data_generation/data/customer_data3.csv")


def create_testing_dog_profile_csv():
    """
    CSV with 20 dogs each testing 2 of the 5 products
    they get paid in treats for their time
    and they all have the status good_dog = True
    """

    product_price = {
        "Carrot Plushy": 5.99,
        "ChewChew Train Dog Bed": 34.99,
        "Where is the Ball? - Transparent Edition": 2.99,
        "Stack of Artisinal Homework": 7.99,
        "Post breakfast treats - calory free": 15.99,
    }

    list_of_dog_breeds = [
        "Labrador Retriever",
        "German Shepherd Dog",
        "Golden Retriever",
        "French Bulldog",
        "Bulldog",
        "Poodle",
        "Beagle",
        "Rottweiler",
        "German Shorthaired Pointer",
        "Pembroke Welsh Corgi",
        "Dachshund",
        "Yorkshire Terrier",
        "Australian Shepherd",
        "Boxer",
        "Siberian Husky",
        "Cavalier King Charles Spaniel",
        "Great Dane",
        "Miniature Schnauzer",
        "Doberman Pinscher",
        "Shiba",
    ]

    # 20 dog names in a list
    dog_names = [
        "Noam Chompsky",
        "Charlie",
        "Luna",
        "Lucy",
        "Max",
        "Barkley",
        "Cooper",
        "Poofpoof",
        "Sadie",
        "Lola",
        "Bud",
        "Furrodosia",
        "Stella",
        "Tucker",
        "Bear",
        "Zoey",
        "Duke",
        "Penny",
        "Sophie",
        "Harley",
    ]

    dog_profile = {
        "DogID": [],
        "Name": [],
        "Breed": [],
        "Age": [],
        "GoodDog": [],
        "Product1": [],
        "Product2": [],
        "Treats": [],
    }

    for i in range(20):
        dog_profile["DogID"].append(uuid.uuid4())
        dog_profile["Name"].append(dog_names[i])
        dog_profile["Breed"].append(random.choice(list_of_dog_breeds))
        dog_profile["Age"].append(random.randint(1, 15))
        dog_profile["GoodDog"].append(True)
        dog_profile["Product1"].append(random.choice(list(product_price.keys())))
        dog_profile["Product2"].append(random.choice(list(product_price.keys())))
        dog_profile["Treats"].append(random.randint(1, 100))

    df = pd.DataFrame(dog_profile)

    df.to_csv("include/data_generation/data/dog_profile3.csv", index=False)
    return dog_profile["DogID"]


create_testing_dog_profile_csv()
