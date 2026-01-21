import db

def dict_from_row(row, cursor):
    """Converts a database row into a dictionary."""
    return dict(zip([col[0] for col in cursor.description], row))

def get_settings():
    """Reads settings from the database."""
    conn = db.get_db_connection()
    settings_data = {}
    try:
        with conn.cursor() as cur:
            # Get monthly_savings_goal
            cur.execute("SELECT value FROM settings WHERE key = 'monthly_savings_goal';")
            goal = cur.fetchone()
            settings_data['monthly_savings_goal'] = float(goal[0]) if goal else 100.0

            # Get expense categories and icons
            cur.execute("SELECT name, icon FROM expense_categories ORDER BY name;")
            expense_categories_rows = cur.fetchall()
            settings_data['expense_categories'] = [row[0] for row in expense_categories_rows]
            settings_data['category_icons'] = {row[0]: row[1] for row in expense_categories_rows}
            settings_data['category_icons']['_default'] = "fa-tags" # Ensure default icon is present

            # Get income categories and icons
            cur.execute("SELECT name, icon FROM income_categories ORDER BY name;")
            income_categories_rows = cur.fetchall()
            settings_data['income_categories'] = [row[0] for row in income_categories_rows]
            settings_data['income_category_icons'] = {row[0]: row[1] for row in income_categories_rows}
            settings_data['income_category_icons']['_default'] = "fa-briefcase" # Ensure default icon is present

    finally:
        db.release_db_connection(conn)
    return settings_data

def update_monthly_savings_goal(goal):
    """Updates the monthly savings goal in the database."""
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES ('monthly_savings_goal', %s) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;",
                (str(goal),)
            )
            conn.commit()
    finally:
        db.release_db_connection(conn)

# --- Expense Category Management ---
def add_expense_category(name, icon):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO expense_categories (name, icon) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING;",
                (name, icon)
            )
            conn.commit()
    finally:
        db.release_db_connection(conn)

def delete_expense_category(name):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM expense_categories WHERE name = %s;", (name,))
            conn.commit()
    finally:
        db.release_db_connection(conn)

def update_expense_category(old_name, new_name, new_icon):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check if new_name already exists and is not the old_name itself
            if old_name != new_name:
                cur.execute("SELECT COUNT(*) FROM expense_categories WHERE name = %s;", (new_name,))
                if cur.fetchone()[0] > 0:
                    return False # New name conflicts with existing category

            cur.execute(
                "UPDATE expense_categories SET name = %s, icon = %s WHERE name = %s;",
                (new_name, new_icon, old_name)
            )
            conn.commit()
            return True
    finally:
        db.release_db_connection(conn)

# --- Income Category Management ---
def add_income_category(name, icon):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO income_categories (name, icon) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING;",
                (name, icon)
            )
            conn.commit()
    finally:
        db.release_db_connection(conn)

def delete_income_category(name):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM income_categories WHERE name = %s;", (name,))
            conn.commit()
    finally:
        db.release_db_connection(conn)

def update_income_category(old_name, new_name, new_icon):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check if new_name already exists and is not the old_name itself
            if old_name != new_name:
                cur.execute("SELECT COUNT(*) FROM income_categories WHERE name = %s;", (new_name,))
                if cur.fetchone()[0] > 0:
                    return False # New name conflicts with existing category

            cur.execute(
                "UPDATE income_categories SET name = %s, icon = %s WHERE name = %s;",
                (new_name, new_icon, old_name)
            )
            conn.commit()
            return True
    finally:
        db.release_db_connection(conn)

def initialize_default_settings():
    """Initializes default settings and categories if tables are empty."""
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            # Default monthly_savings_goal
            cur.execute("SELECT COUNT(*) FROM settings WHERE key = 'monthly_savings_goal';")
            if cur.fetchone()[0] == 0:
                cur.execute("INSERT INTO settings (key, value) VALUES ('monthly_savings_goal', '100.0');")

            # Default Expense Categories
            default_expense_categories = [
                ("Food", "fa-utensils"), ("Drink", "fa-mug-saucer"), ("Coffee", "fa-coffee"),
                ("Transportation", "fa-car"), ("Rent", "fa-house"), ("Utilities", "fa-lightbulb"),
                ("Shopping", "fa-bag-shopping"), ("Entertainment", "fa-film"), ("Gym", "fa-dumbbell"),
                ("Event", "fa-calendar-check"), ("Petroleum", "fa-gas-pump"), ("Family", "fa-people-group"),
                ("Goal Savings", "fa-piggy-bank"), ("Annual Trip", "fa-plane"), ("Haircut", "fa-cut"),
                ("Other", "fa-ellipsis-h")
            ]
            for name, icon in default_expense_categories:
                cur.execute("INSERT INTO expense_categories (name, icon) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING;", (name, icon))

            # Default Income Categories
            default_income_categories = [
                ("Salary", "fa-money-bill-wave"), ("Bonus", "fa-gift"), ("Freelance", "fa-laptop-code"),
                ("Other", "fa-search-dollar")
            ]
            for name, icon in default_income_categories:
                cur.execute("INSERT INTO income_categories (name, icon) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING;", (name, icon))
            
            conn.commit()
    finally:
        db.release_db_connection(conn)
