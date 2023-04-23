from sqlalchemy import select, func, literal, Numeric, desc, asc, case
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

    def order_details_extended(self):
        """
        This query calculates sales price for each order after discount is applied.
        """
        query = select(
            self.order_details.order_id,
            self.products.product_id,
            self.products.product_name,
            func.cast(func.sum(
                self.order_details.unit_price * self.order_details.quantity * (1 - self.order_details.discount)
            ), Numeric(precision=10, scale=2)).label('sales_amount')
        ).join(self.order_details). \
            group_by(self.order_details.order_id, self.products.product_id). \
            order_by(self.order_details.order_id). \
            distinct()

        return self.session.execute(query).fetchall()

    def sales_by_category(self):
        """
        For each category, we get the list of products sold and the total sales amount.
        Note that, in the second query, the inner query for table c is to get sales for each product on each order. It then joins with outer query on Product_ID. In the outer query, products are grouped for each category.
        """
        subquery = select(
            self.order_details.order_id,
            self.products.product_id,
            self.products.product_name,
            func.cast(func.sum(
                self.order_details.unit_price * self.order_details.quantity * (1 - self.order_details.discount)
            ), Numeric(precision=10, scale=2)).label('extend_price')
        ).join(self.order_details). \
            group_by(self.order_details.order_id, self.products.product_id). \
            order_by(self.order_details.order_id). \
            distinct(). \
            subquery()
        query = select(
            self.categories.category_id,
            self.products.product_name,
            self.categories.category_name,
            func.sum(subquery.c.extend_price).label('product_sales')
        ).join(self.products, self.products.category_id == self.categories.category_id). \
            join(subquery, subquery.c.product_id == self.products.product_id). \
            join(self.orders, self.orders.order_id == subquery.c.order_id). \
            where(and_(
            self.orders.order_date >= '1997-1-1',
            self.orders.order_date <= "1997-12-31"
        )). \
            group_by(
            self.categories.category_id,
            self.categories.category_name,
            self.products.product_name
        ). \
            order_by(
            self.categories.category_id,
            self.categories.category_name,
            self.products.product_name
        )
        return self.session.execute(query).fetchall()

    def ten_most_expensive_products(self):
        query = select(
            self.products.product_name,
            self.products.unit_price
        ).order_by(desc(self.products.unit_price)).distinct().limit(10)
        return self.session.execute(query).fetchall()

    def product_by_category(self):
        query = select(
            self.categories.category_name,
            self.products.product_name
        ).join(self.categories). \
            where(self.products.discontinued == 1). \
            group_by(
            self.categories.category_name,
            self.products.product_name
        ). \
            order_by(
            self.categories.category_name,
            self.products.product_name
        )
        return self.session.execute(query).fetchall()

    def customer_and_suppliers_by_city(self):
        query_1 = select(
            self.customers.city,
            self.customers.contact_name,
            self.customers.company_name,
        )
        query_2 = select(
            self.suppliers.city,
            self.suppliers.contact_name,
            self.suppliers.company_name,
        )
        query = query_1.union(
            query_2
        ).order_by(self.customers.city, self.suppliers.company_name)
        print(query)
        return self.session.execute(query).fetchall()

    def products_above_average_price(self):
        subquery = select(
            func.avg(self.products.unit_price)
        ).scalar_subquery()
        query = select(
            self.products.product_name,
            self.products.unit_price
        ).where(self.products.unit_price > subquery). \
            order_by(self.products.unit_price)
        return self.session.execute(query).fetchall()

    def product_sales_for_1997(self):
        query = select(
            self.categories.category_name,
            self.products.product_name,
            func.cast(func.sum(
                self.order_details.unit_price * self.order_details.quantity * (1 - self.order_details.discount)
            ), Numeric(precision=10, scale=2)).label('extend_price'),
            func.extract('quarter', self.orders.shipped_date).label('shipped_quarter')
        ).join(self.products, self.categories.category_id == self.categories.category_id). \
            join(self.order_details, self.order_details.product_id == self.products.product_id). \
            join(self.orders, self.orders.order_id == self.order_details.order_id). \
            where(and_(
            self.orders.shipped_date >= '1997-01-01',
            self.orders.shipped_date <= '1997-12-31'
        )). \
            group_by(self.categories.category_name, self.products.product_name, 'shipped_quarter'). \
            order_by(self.categories.category_name, self.products.product_name, 'shipped_quarter'). \
            distinct()
        return self.session.execute(query).fetchall()

    def quarterly_orders_by_product(self):
        sum_query = func.cast(func.sum(
            self.order_details.unit_price * self.order_details.quantity * (1 - self.order_details.discount)
        ), Numeric(precision=10, scale=2))

        def sub_case(quart: str) -> case:
            return case((
                func.extract('quarter', self.orders.order_date) == quart,
                sum_query
            ),
                else_=0
            ).label(f'qtr {quart}')

        query = select(
            self.products.product_name,
            self.customers.company_name,
            func.extract('year', self.orders.order_date).label('order_year'),
            sub_case('1'),
            sub_case('2'),
            sub_case('3'),
            sub_case('4'),
        ).join(self.order_details, self.order_details.product_id == self.products.product_id). \
            join(self.orders, self.orders.order_id == self.order_details.order_id). \
            join(self.customers, self.customers.customer_id == self.orders.customer_id). \
            where(and_(
            self.orders.shipped_date >= '1997-01-01',
            self.orders.shipped_date <= '1997-12-31'
        )). \
            group_by(
            self.orders.order_date,
            self.products.product_name,
            self.customers.company_name,
        ). \
            order_by(
            self.products.product_name,
            self.customers.company_name
        )
        return self.session.execute(query).fetchall()
