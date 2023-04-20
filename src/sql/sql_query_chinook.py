from sqlalchemy import select, func, and_, or_, extract, desc, case, null
from sqlalchemy.ext.automap import AutomapBase
from sqlalchemy.orm import Session

from src.sql.base_sql_query import BaseSQLQuery


class SQLQueryChinook(BaseSQLQuery):
    """
    All questions get from https://github.com/brooksquil/sqlite-assignment-chinook
    """

    def __init__(self, base: AutomapBase, session: Session) -> None:
        super().__init__(base=base, session=session)
        self.employee = base.classes['Employee']
        self.customer = base.classes['Customer']
        self.invoice = base.classes['Invoice']
        self.invoice_line = base.classes['InvoiceLine']
        self.track = base.classes['Track']
        self.playlist = base.classes['Playlist']
        self.artist = base.classes['Artist']
        self.album = base.classes['Album']
        self.media_type = base.classes['MediaType']
        self.genre = base.classes['Genre']

    def non_usa_customers(self):
        """
        Provide a query showing Customers (just their full names, customer ID and country) who are not in the US.
        :return:
        """
        query = select(
            self.customer.FirstName,
            self.customer.LastName,
            self.customer.CustomerId,
            self.customer.Country
        ).where(self.customer.Country != 'USA')
        return self.session.execute(query).fetchall()

    def brazil_customers(self):
        """
        Provide a query only showing the Customers from Brazil.
        :return:
        """
        query = select(
            self.customer
        ).where(self.customer.Country == 'Brazil')
        return self.session.execute(query).fetchall()

    def brasil_customers_invoices(self):
        """
        Provide a query showing the Invoices for customers who are from Brazil.
        The resultant table should show the customer's full name,
        Invoice ID, Date of the invoice and billing country.
        :return:
        """
        query = select(
            self.customer.FirstName,
            self.customer.LastName,
            self.invoice.InvoiceId,
            self.invoice.InvoiceDate,
            self.invoice.BillingCountry
        ).join(self.invoice). \
            where(self.customer.Country == 'Brazil')
        return self.session.execute(query).fetchall()

    def sales_agents(self):
        """
        Provide a query showing only the Employees who are Sales Agents.
        :return:
        """
        query = select(
            self.employee.FirstName,
            self.employee.LastName
        ). \
            where(self.employee.Title == 'Sales Support Agent')
        return self.session.execute(query).fetchall()

    def sales_agent_with_case_when(self):
        query = select(
            self.employee.EmployeeId,
            func.count(),
            case(
                (self.employee.EmployeeId == 4, 1),
                (self.employee.EmployeeId == 5, 1),
                (self.employee.EmployeeId == 3, 1),
                else_=null()
            )
        ). \
            group_by(self.employee.EmployeeId)
        print(query)
        return self.session.execute(query).fetchall()

    #
    # case(
    #     (self.employee.EmployeeId == 4, self.invoice.Total), else_=null()
    # ).label('Margaret_Park'). \
    #     case(
    #     (self.employee.EmployeeId == 5, self.invoice.Total), else_=null()
    # ).label('Steve_Johnson')
    def unique_invoice_countries(self):
        """
        Provide a query showing a unique/distinct list of billing countries from the Invoice table.
        :return:
        """
        query = select(
            self.invoice.BillingCountry,
        ).distinct()
        print(query)
        return self.session.execute(query).fetchall()

    def sales_agent_invoices(self):
        """
        Provide a query that shows the invoices associated with each sales agent.
        The resultant table should include the Sales Agent's full name.
        :return:
        """
        query = select(
            self.employee.LastName,
            self.employee.FirstName,
            self.invoice.InvoiceId
        ).join(self.customer, self.customer.SupportRepId == self.employee.EmployeeId). \
            join(self.invoice)
        return self.session.execute(query).fetchall()

    def invoice_totals(self):
        """
         Provide a query that shows the Invoice Total, Customer name,
         Country and Sale Agent name for all invoices and customers.
        :return:
        """
        query = select(
            self.employee.LastName,
            self.employee.FirstName,
            self.customer.LastName,
            self.customer.Country,
            self.invoice.Total
        ).join(self.customer, self.customer.SupportRepId == self.employee.EmployeeId). \
            join(self.invoice)
        return self.session.execute(query).fetchall()

    def total_invoices_year(self):
        """
        How many Invoices were there in 2009 and 2011?
        :return:
        """

        query = select(
            func.count()
        ).filter(or_(
            and_(self.invoice.InvoiceDate >= '2009-01-01', self.invoice.InvoiceDate <= '2009-12-31'),
            and_(self.invoice.InvoiceDate >= '2011-01-01', self.invoice.InvoiceDate <= '2011-12-31')
        ))
        return self.session.execute(query).fetchone()

    def total_sales(self):
        """
        What are the respective total sales for each of those years?
        :return:
        """
        query = select(
            func.sum(self.invoice.Total)
        ).filter(or_(
            and_(self.invoice.InvoiceDate >= '2009-01-01', self.invoice.InvoiceDate <= '2009-12-31'),
            and_(self.invoice.InvoiceDate >= '2011-01-01', self.invoice.InvoiceDate <= '2011-12-31')
        ))
        return self.session.execute(query).fetchone()

    def invoice_37_line_item_count(self):
        """
        Looking at the InvoiceLine table, provide a query that COUNT the number of line items for Invoice ID 37.
        :return:
        """
        query = select(
            func.count()
        ).filter(self.invoice_line.InvoiceId == 37)
        return self.session.execute(query).fetchall()

    def line_items_per_invoice(self):
        """
        Looking at the InvoiceLine table,
        provide a query that COUNT the number of line items for each Invoice. HINT: GROUP BY
        :return:
        """
        query = select(
            self.invoice_line.InvoiceId,
            func.count(self.invoice_line.InvoiceLineId)
        ).group_by(self.invoice_line.InvoiceId)
        return self.session.execute(query).fetchall()

    def line_item_track(self):
        """
         Provide a query that includes the purchased track name with each invoice line item.
        :return:
        """
        query = select(
            self.track.Name,
            self.invoice_line
        ).join(self.track, self.track.TrackId == self.invoice_line.TrackId)
        return self.session.execute(query).fetchall()

    def line_item_track_artist(self):
        """
        Provide a query that includes the purchased track name AND artist name with each invoice line item.
        :return:
        """
        query = select(
            self.track.Name,
            self.track.Composer,
            self.invoice_line.InvoiceLineId
        ).join(self.track)
        return self.session.execute(query).fetchall()

    def country_invoices(self):
        """
        Provide a query that shows the # of invoices per country. HINT: GROUP BY
        :return:
        """
        query = select(
            self.invoice.BillingCountry,
            func.count(self.invoice.InvoiceId)
        ).group_by(self.invoice.BillingCountry)
        return self.session.execute(query).fetchall()

    def tracks_no_id(self):
        """
        Provide a query that shows all the Tracks, but displays no IDs.
        The result should include the Album name, Media type and Genre.
        :return:
        """
        query = select(
            self.track.Name,
            self.genre.Name,
            self.media_type.Name,
            self.album.Title
        ).join(self.album). \
            join(self.media_type). \
            join(self.genre)
        return self.session.execute(query).fetchall()

    def invoices_line_item_count(self):
        """
        Provide a query that shows all Invoices but includes the # of invoice line items.
        :return:
        """
        query = select(
            self.invoice.InvoiceId,
            func.count(self.invoice.InvoiceId)
        ).group_by(self.invoice.InvoiceId)
        return self.session.execute(query).fetchall()

    def sales_agent_total_sales(self):
        """
        Provide a query that shows total sales made by each sales agent.
        :return:
        """
        query = select(
            self.employee.EmployeeId,
            self.employee.FirstName,
            func.sum(self.invoice.Total)
        ).join(self.customer, self.customer.SupportRepId == self.employee.EmployeeId). \
            join(self.invoice, self.customer.CustomerId == self.invoice.CustomerId). \
            where(self.employee.Title == 'Sales Support Agent'). \
            group_by(self.employee.EmployeeId)
        return self.session.execute(query).fetchall()

    def top_2009_agent(self):
        """
        Which sales agent made the most in sales in 2009?

        :return:
        """
        query = select(
            self.employee.EmployeeId,
            self.employee.FirstName,
            func.sum(self.invoice.Total).label("sum_")
        ).join(self.customer, self.customer.SupportRepId == self.employee.EmployeeId). \
            join(self.invoice, self.customer.CustomerId == self.invoice.CustomerId). \
            where(self.employee.Title == 'Sales Support Agent'). \
            where(extract('year', self.invoice.InvoiceDate) == 2009). \
            group_by(self.employee.EmployeeId). \
            order_by(desc('sum_')). \
            limit(1)
        return self.session.execute(query).fetchall()

    def extract_date_between_date(self):
        query_1 = select(
            func.count()
        ).where(and_(self.invoice.InvoiceDate >= '2009-01-01', self.invoice.InvoiceDate <= '2009-12-31'))
        query_2 = select(
            func.count()
        ).where(extract('year', self.invoice.InvoiceDate) == 2009)
        print(query_1)
        print(query_2)
        print(self.session.execute(query_1).fetchall())
        print(self.session.execute(query_2).fetchall())

    def sales_agent_customer_count(self):
        """
         Provide a query that shows the count of customers assigned to each sales agent.
        :return:
        """
        query = select(
            self.employee.EmployeeId,
            self.employee.FirstName,
            func.count(self.customer.CustomerId),
        ).join(self.customer, self.customer.SupportRepId == self.employee.EmployeeId). \
            group_by(self.employee.EmployeeId)
        return self.session.execute(query).fetchall()

    def sales_per_country(self):
        """
        Provide a query that shows the total sales per country.
        :return:
        """
        query = select(
            self.customer.Country,
            func.sum(self.invoice.Total)
        ).join(self.customer). \
            group_by(self.customer.Country)
        return self.session.execute(query).fetchall()

    def top_country(self):
        """
        Which country's customers spent the most?
        :return:
        """
        query = select(
            self.customer.Country,
            func.sum(self.invoice.Total).label('total')
        ).join(self.customer). \
            group_by(self.customer.Country). \
            order_by(desc('total')). \
            limit(1)
        return self.session.execute(query).fetchall()

    def top_2013_track(self):
        """
        Provide a query that shows the most purchased track of 2013.
        :return:
        """
        query = select(
            self.track.Name,
            func.sum(self.invoice_line.InvoiceId).label('total')
        ).join(self.invoice, self.invoice.InvoiceId == self.invoice_line.InvoiceId). \
            join(self.track, self.track.TrackId == self.invoice_line.TrackId). \
            where(extract('year', self.invoice.InvoiceDate) == 2013). \
            group_by(self.track.Name). \
            order_by(desc('total')). \
            limit(10)

        return self.session.execute(query).fetchall()

    def top_5_tracks(self):
        """
         Provide a query that shows the top 5 most purchased tracks over all.
        :return:
        """
        query = select(
            self.track.Name,
            func.sum(self.invoice_line.InvoiceId).label('total')
        ).join(self.invoice, self.invoice.InvoiceId == self.invoice_line.InvoiceId). \
            join(self.track, self.track.TrackId == self.invoice_line.TrackId). \
            group_by(self.track.Name). \
            order_by(desc('total')). \
            limit(5)

        return self.session.execute(query).fetchall()

    def top_3_artists(self):
        """
        Provide a query that shows the top 3 best-selling artists.
        :return:
        """
        query = select(
            self.artist.Name,
            func.sum(self.invoice_line.InvoiceId).label('total')
        ).join(self.invoice, self.invoice.InvoiceId == self.invoice_line.InvoiceId). \
            join(self.track, self.track.TrackId == self.invoice_line.TrackId). \
            join(self.album, self.album.AlbumId == self.track.AlbumId). \
            join(self.artist, self.artist.ArtistId == self.album.AlbumId). \
            group_by(self.artist.Name). \
            order_by(desc('total')). \
            limit(3)

        return self.session.execute(query).fetchall()

    def top_media_type(self):
        """
        Provide a query that shows the most purchased Media Type.# sqlite-assignment-chinook
        :return:
        """
        query = select(
            self.media_type.Name,
            func.count(self.media_type.Name).label('count')
        ).join(self.track, self.track.MediaTypeId == self.media_type.MediaTypeId). \
            join(self.invoice_line, self.invoice_line.TrackId == self.track.TrackId). \
            group_by(self.media_type.Name). \
            order_by(desc('count')). \
            limit(1)
        return self.session.execute(query).fetchall()
