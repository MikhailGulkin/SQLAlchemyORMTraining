from sqlalchemy.ext.automap import AutomapBase
from sqlalchemy.orm import Session

from src.sql.base_sql_query import BaseSQLQuery


class SQLQueryNorthwind(BaseSQLQuery):
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
