from datetime import datetime, timedelta
import db

def dict_from_row(row, cursor):
    """Converts a database row into a dictionary."""
    return dict(zip([col[0] for col in cursor.description], row))

def add_transaction(type, category, item, amount, date, description, savings_goal_id=None):
    """Adds a single transaction to the database."""
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            # The savings_goal_id can be None, so handle that case
            if savings_goal_id == '' or savings_goal_id is None:
                goal_id_to_insert = None
            else:
                goal_id_to_insert = int(savings_goal_id)

            cur.execute(
                "INSERT INTO transactions (date, type, category, item, amount, description, savings_goal_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s);",
                (date, type, category, item, amount, description, goal_id_to_insert)
            )
            conn.commit()
    finally:
        db.release_db_connection(conn)

def get_transactions(sort_by_date=True):
    """Reads all transactions from the database."""
    conn = db.get_db_connection()
    transactions = []
    try:
        with conn.cursor() as cur:
            query = "SELECT * FROM transactions"
            if sort_by_date:
                query += " ORDER BY date DESC, transaction_id DESC"
            cur.execute(query)
            for row in cur.fetchall():
                transactions.append(dict_from_row(row, cur))
    finally:
        db.release_db_connection(conn)
    return transactions

def get_transaction(transaction_id):
    """Retrieves a single transaction by its ID from the database."""
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM transactions WHERE transaction_id = %s;", (transaction_id,))
            row = cur.fetchone()
            if row:
                return dict_from_row(row, cur)
    finally:
        db.release_db_connection(conn)
    return None

def delete_transaction(transaction_id):
    """Deletes a transaction by its ID from the database."""
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM transactions WHERE transaction_id = %s;", (transaction_id,))
            conn.commit()
    finally:
        db.release_db_connection(conn)

def update_transaction(transaction_id, data):
    """Updates a transaction by its ID in the database."""
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            # Handle case where savings_goal_id might be an empty string
            if 'savings_goal_id' in data and data['savings_goal_id'] == '':
                data['savings_goal_id'] = None
            
            cur.execute(
                "UPDATE transactions SET date=%s, type=%s, category=%s, item=%s, amount=%s, description=%s, savings_goal_id=%s "
                "WHERE transaction_id = %s;",
                (
                    data['date'], data['type'], data['category'], data['item'],
                    data['amount'], data['description'], data.get('savings_goal_id'),
                    transaction_id
                )
            )
            conn.commit()
    finally:
        db.release_db_connection(conn)

def generate_report_data(period=None, start_date_str=None, end_date_str=None):
    """Generates budget report data for a given period or custom date range using database queries."""
    today = datetime.now()
    
    if start_date_str and end_date_str:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    else:
        if period == 'daily':
            start_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
        elif period == 'weekly':
            start_date = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(weeks=1)
        elif period == 'yearly':
            start_date = today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date.replace(year=today.year + 1)
        else: # Default to monthly
            period = 'monthly'
            start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            next_month = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1)
            end_date = next_month

    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            # Fetch filtered transactions
            cur.execute(
                "SELECT * FROM transactions WHERE date >= %s AND date < %s ORDER BY date DESC, transaction_id DESC;",
                (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            )
            filtered_transactions = [dict_from_row(row, cur) for row in cur.fetchall()]

            # Fetch aggregated data
            cur.execute(
                "SELECT type, category, SUM(amount) as total FROM transactions "
                "WHERE date >= %s AND date < %s GROUP BY type, category;",
                (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            )
            summary_data = cur.fetchall()
            
            total_income = sum(s[2] for s in summary_data if s[0] == 'income')
            total_expense = sum(s[2] for s in summary_data if s[0] == 'expense')
            total_goal_savings = sum(s[2] for s in summary_data if s[1] == 'Goal Savings')
            total_general_savings = sum(s[2] for s in summary_data if s[1] == 'General Savings')
            
            # Income breakdown by item
            cur.execute(
                "SELECT item, SUM(amount) as total FROM transactions "
                "WHERE type = 'income' AND date >= %s AND date < %s "
                "GROUP BY item ORDER BY total DESC;",
                (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            )
            income_breakdown_by_item = {row[0]: float(row[1]) for row in cur.fetchall()}

            # Monthly summaries for yearly report
            monthly_summaries = []
            if period == 'yearly':
                cur.execute(
                    "SELECT TO_CHAR(date, 'YYYY-MM') as month, type, SUM(amount) "
                    "FROM transactions WHERE date >= %s AND date < %s "
                    "GROUP BY month, type ORDER BY month;",
                    (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                )
                month_data = {}
                for row in cur.fetchall():
                    month, trans_type, total = row
                    if month not in month_data:
                        month_data[month] = {'total_income': 0, 'total_expense': 0}
                    if trans_type == 'income':
                        month_data[month]['total_income'] = float(total)
                    else:
                        month_data[month]['total_expense'] += float(total)

                for month, values in sorted(month_data.items()):
                    monthly_summaries.append({
                        'month': month,
                        'total_income': values['total_income'],
                        'total_expense': values['total_expense'],
                        'balance': values['total_income'] - values['total_expense']
                    })

    finally:
        db.release_db_connection(conn)

    return {
        "period": period,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": (end_date - timedelta(seconds=1)).strftime('%Y-%m-%d'),
        "total_income": float(total_income),
        "total_expense": float(total_expense),
        "total_savings": float(total_goal_savings + total_general_savings),
        "total_goal_savings": float(total_goal_savings),
        "total_general_savings": float(total_general_savings),
        "balance": float(total_income - total_expense),
        "transactions": filtered_transactions,
        "income_breakdown_by_item": income_breakdown_by_item,
        "monthly_summaries": monthly_summaries
    }