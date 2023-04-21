from sqlalchemy import select, func, literal, Numeric, desc, asc
from sqlalchemy.ext.automap import AutomapBase
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import and_

from src.sql.base_sql_query import BaseSQLQuery


class SQLQueryNorthwind(BaseSQLQuery):
    """
    All questions from https://www.geeksengine.com/database/problem-solving/northwind-queries-part-1.php
    """

    def __init__(self, base: AutomapBase, session: Session) -> None:
        super().__init__(base=base, session=session)

        self.categories = self.base.classes['categories']
        self.products = self.base.classes['products']
        self.suppliers = self.base.classes['suppliers']
        self.orders = self.base.classes['orders']
        self.order_details = self.base.classes['order_details']
        self.customers = self.base.classes['customers']
        # self.customer_customer_demo = self.base.classes['customer_customer_demo']
        self.customer_demographics = self.base.classes['customer_demographics']
        self.shippers = self.base.classes['shippers']
        self.employees = self.base.classes['employees']
        # self.employee_territories = self.base.classes['employee_territories']
        self.territories = self.base.classes['territories']
        self.region = self.base.classes['region']
        self.us_states = self.base.classes['us_states']

    def order_subtotals(self):
        """
        For each order, calculate a subtotal for each Order (identified by OrderID).
        This is a simple query using GROUP BY to aggregate data for each order.
        """

        query = select(
            self.order_details.order_id,
            func.cast(func.sum(
                self.order_details.unit_price * self.order_details.quantity * (1 - self.order_details.discount)
            ), Numeric(precision=10, scale=2))
        ).group_by(self.order_details.order_id). \
            order_by(self.order_details.order_id)

        return self.session.execute(query).fetchall()

    def sales_by_year(self):
        """
        This query shows how to get the year part from Shipped_Date column.
        A subtotal is calculated by a sub-query for each order.
        The sub-query forms a table and then joined with the Orders table.
        """
        sub_query = select(
            self.order_details.order_id,
            func.cast(func.sum(
                self.order_details.unit_price * self.order_details.quantity * (1 - self.order_details.discount)
            ), Numeric(precision=10, scale=2)).label('sub_total')
        ).group_by(self.order_details.order_id). \
            order_by(self.order_details.order_id).subquery()

        query = select(
            self.orders.shipped_date,
            self.orders.order_id,
            sub_query.c.sub_total,
            func.extract("year", self.orders.shipped_date).label('year')
        ).join(sub_query, sub_query.c.order_id == self.orders.order_id). \
            where(and_(
            self.orders.shipped_date != None,
            self.orders.shipped_date >= "1996-12-24",
            self.orders.shipped_date <= "1997-09-30"
        )).order_by(asc(self.orders.shipped_date), desc(self.orders.order_id))

        return self.session.execute(query).fetchall()

    def employee_sales_by_country(self):
        """
        For each employee, get their sales amount, broken down by country name.
        """
        query = select(
            self.customers.country,
            self.employees.first_name,
            self.employees.last_name,
            self.orders.order_id,
            func.cast(func.sum(
                self.order_details.unit_price * self.order_details.quantity * (1 - self.order_details.discount)
            ), Numeric(precision=10, scale=2)).label('sales_amount')
        ).join(self.orders, self.orders.employee_id == self.employees.employee_id). \
            join(self.customers, self.customers.customer_id == self.orders.customer_id). \
            join(self.order_details, self.order_details.order_id == self.orders.order_id). \
            group_by(self.employees.employee_id, self.orders.order_id, self.customers.country). \
            order_by(desc(self.employees.employee_id), self.customers.country)
        return self.session.execute(query).fetchall()

    def alphabetical_list_of_products(self):
        """
        This is a rather simple query to get an alphabetical list of products.
        """
        query = select(
            self.products
        ).order_by(
            self.products.product_name
        )
        return self.session.execute(query).fetchall()

    def current_product_list(self):
        """
        This is a rather simple query to get an alphabetical list of products.
        """
        query = select(
            self.products.product_name
        ).where(self.products.discontinued == 1)
        return self.session.execute(query).fetchall()
