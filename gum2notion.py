import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime
import time


class GumToNotion:
	def __init__(self):
		load_dotenv()

		# Gumroad related
		self.gumroad_token = os.getenv("gumroad_access_token")
		self.gumroad_url = "https://api.gumroad.com/v2"
		
		# Notion related
		self.notion_token = os.getenv("notion_access_token")
		self.database_id = os.getenv("database_id")
		self.notion_headers = {
			"Authorization": f"Bearer {self.notion_token}",
			"Content-Type": "application/json",
			"Notion-Version": "2022-06-28",
		}

		self.db = self.read_database()

	def response_database(self):
		"""Check the status of the database connection."""
		read_url = f"{self.gumroad_url}/products"
		response = requests.get(read_url)
		print(response.status_code)

	def read_database(self):
		"""Read entries from the Notion database."""
		read_url = f"https://api.notion.com/v1/databases/{self.database_id}/query"
		response = requests.post(read_url, headers=self.notion_headers)
		data = response.json()
		return data

	def convert_notion_created_time(self, created_time):
		"""Convert Notion created time to datetime object."""
		return datetime.strptime(created_time, "%Y-%m-%dT%H:%M:%S.%fZ")


	def get_notion_recent_sale_time(self):
		"""Retrieve the creation date of the most recent entry in the Notion database."""
		recent_sale = ''.join(self.db["results"][0]["created_time"])
		return self.convert_notion_created_time(recent_sale)


	def get_gumroad_new_sales(self):
		"""Retrieve sales that occurred after the specified time."""
		gumroad_url = f"{self.gumroad_url}/sales"
		after_date = self.get_notion_recent_sale_time().strftime("%Y-%m-%d")
		gumroad_payload = {
			"access_token": self.gumroad_token,
			"after": after_date,
		}
		gumroad_response = requests.get(gumroad_url, params=gumroad_payload)

		sales_data = gumroad_response.json()
		return sales_data
		
	def recent_sale_not_in_db(self):
		"""Retrieve sales that occurred after the most recent entry in the Notion database."""
		new_sales = self.get_gumroad_new_sales()
		last_run_time = self.get_notion_recent_sale_time()

		recent_customers = []

		for sale_entry in new_sales["sales"]:
			recent_sale = sale_entry["created_at"]
			recent_sale_time = datetime.strptime(recent_sale, "%Y-%m-%dT%H:%M:%SZ")

			if recent_sale_time > last_run_time:
				recent_customers.append(sale_entry)

		filtered_sales = {"success": True, "sales": recent_customers}
		return filtered_sales
##
	def check_existing_customer(self, email):
		"""Check if a customer with the given email already exists in the Notion database."""
		customer_db = self.db["results"]
		for customer in customer_db:
			check_email = customer["properties"]["﻿email address"]["email"]
			if email == check_email:
				return customer
		return None


	def add_customer_to_notion(self, price, is_true, product_name, email):
		"""Add a new customer entry to the Notion database."""
		update_db_url = f'https://api.notion.com/v1/databases/{self.database_id}'
		update_page_url = "https://api.notion.com/v1/pages"
		new_price = round(price/100, 2)

		new_page = {
			"price spent": {"number": new_price},
			"subscribed": {"checkbox": is_true},
			"purchased products": {"multi_select": [{"name": product_name}]},
			"﻿email address": {"email": email},
		}

		parent = {"database_id": self.database_id}

		data = {"parent": parent, "properties": new_page}

		response = requests.post(update_page_url, headers=self.notion_headers, data=json.dumps(data))

	def add_recent_customers(self):
		"""Add recent customers to the Notion database."""
		recent_sales = self.recent_sale_not_in_db()

		for recent_customer in recent_sales["sales"]:
			price = float(recent_customer["price"])
			is_true = recent_customer["can_contact"]
			product_name = recent_customer["product_name"]
			email = recent_customer["purchase_email"]
			self.add_customer_to_notion(price, is_true, product_name, email)

   
	def multi_select_name(self, multi_select):
		"""Extract names from multi-select data."""
		all_name = []
		for select in multi_select:
			name = select["name"]
			all_name.append({"name": name})
		return all_name

	def merge_duplicate_emails(self):
		"""Merge data for customers with duplicate emails in the Notion database."""
		updated_db = self.read_database()

		email_records = {}

		for record in updated_db["results"]:
			email = record["properties"]["﻿email address"]["email"]

			if email in email_records:
				email_record_created_time = self.convert_notion_created_time(email_records[email]["created_time"])
				record_created_time = self.convert_notion_created_time(record["created_time"])
				record_product_names = record["properties"]["purchased products"]["multi_select"]
				new_item = self.multi_select_name(record_product_names)

				email_records[email]["price_spent"] += float(record["properties"]["price spent"]["number"])
				email_records[email]["purchased_products"].extend(new_item)

				if record_created_time > email_record_created_time:
					new_id = record["id"]
					old_ids = email_records[email].get("duplicate_id", [])
					old_ids.append(email_records[email]["id"])

					email_records[email]["duplicate_id"] = old_ids
					email_records[email]["id"] = new_id
					email_records[email]["subscribed"] = (record["properties"]["subscribed"]["checkbox"].lower() == "true")
					email_records[email]["created_time"] = record["created_time"]
				else:
					duplicate_ids = email_records[email].get("duplicate_id", [])
					duplicate_ids.append(record["id"])
					email_records[email]["duplicate_id"] = duplicate_ids

			else:
				price = float(record["properties"]["price spent"]["number"])
				is_true = record["properties"]["subscribed"]["checkbox"]
				purchased_products = self.multi_select_name(record["properties"]["purchased products"]["multi_select"])
				email_records[email] = {
					"price_spent": price,
					"subscribed": is_true,
					"purchased_products": purchased_products,
					"created_time": record["created_time"],
					"id": record["id"],
				}

		for email, data in email_records.items():
			if "duplicate_id" in data:
				update_url = f'https://api.notion.com/v1/pages/{data["id"]}'
				data = {
					"properties": {
						"price spent": {"number": data["price_spent"]},
						"subscribed": {"checkbox": data["subscribed"]},
						"purchased products": {"multi_select": data["purchased_products"]},
					}
				}
				updated_page_data = json.dumps(data)
				res = requests.patch(update_url, headers=self.notion_headers, data=updated_page_data)

		for email, data in email_records.items():
			if "duplicate_id" in data:
				for duplicate_id in data.get("duplicate_id", []):
					delete_url = f'https://api.notion.com/v1/pages/{duplicate_id}'
					payload = {"archived": True}
					res = requests.patch(delete_url, headers=self.notion_headers, json=payload)
	
	def update_indefinitely(self):
		"""Update Notion entries indefinitely."""
		while True:
			try:
				# Add recent customers to the Notion database
				self.add_recent_customers()
				# Merge duplicate emails in the Notion database
				self.merge_duplicate_emails()
				print("test")
				time.sleep(300)  # Adjust sleep time as needed
			except Exception:
				traceback.print_exception(*sys.exc_info())
				time.sleep(60)  # Adjust sleep time after an exception as needed

if __name__ == "__main__":
	# With 😴 sleeps to prevent rate limit from kicking in.
	GumToNotion().update_indefinitely()