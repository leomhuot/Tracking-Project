from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from flask_mail import Mail, Message
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature
import budget as budget_logic
import settings_manager
import savings_goals as savings_goals_logic
from datetime import datetime
import os
import pyotp
import base64
from functools import wraps

import db  # Import the new db module

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['MAIL_SERVER'] = os.environ.get('MAIL_SERVER')
app.config['MAIL_PORT'] = int(os.environ.get('MAIL_PORT', 587))
app.config['MAIL_USE_TLS'] = os.environ.get('MAIL_USE_TLS', 'True').lower() == 'true'
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.environ.get('MAIL_DEFAULT_SENDER')

# Initialize the database
with app.app_context():
    db.init_db()

mail = Mail(app)
s = URLSafeTimedSerializer(app.secret_key)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != 'admin':
            flash('You do not have permission to access this page.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

class User(UserMixin):
    def __init__(self, id, username, email, password_hash, role='user', totp_secret=None):
        self.id = id
        self.username = username
        self.email = email
        self.password_hash = password_hash
        self.role = role
        self.totp_secret = totp_secret

def get_user_by_username(username):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE username = %s;", (username,))
            user_data = cur.fetchone()
            if user_data:
                return User(id=user_data[0], username=user_data[1], email=user_data[2], password_hash=user_data[3], role=user_data[4], totp_secret=user_data[5])
    finally:
        db.release_db_connection(conn)
    return None

def get_user_by_email(email):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE email = %s;", (email,))
            user_data = cur.fetchone()
            if user_data:
                return User(id=user_data[0], username=user_data[1], email=user_data[2], password_hash=user_data[3], role=user_data[4], totp_secret=user_data[5])
    finally:
        db.release_db_connection(conn)
    return None

def get_user_by_id(user_id):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s;", (user_id,))
            user_data = cur.fetchone()
            if user_data:
                return User(id=user_data[0], username=user_data[1], email=user_data[2], password_hash=user_data[3], role=user_data[4], totp_secret=user_data[5])
    finally:
        db.release_db_connection(conn)
    return None

@login_manager.user_loader
def load_user(user_id):
    return get_user_by_id(user_id)

def get_all_users():
    users = []
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users ORDER BY id;")
            for row in cur.fetchall():
                users.append(User(id=row[0], username=row[1], email=row[2], password_hash=row[3], role=row[4], totp_secret=row[5]))
    finally:
        db.release_db_connection(conn)
    return users

def update_user_totp_secret(user_id, totp_secret):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET totp_secret = %s WHERE id = %s;", (totp_secret, user_id))
            conn.commit()
    finally:
        db.release_db_connection(conn)

def update_user_password(user_id, new_password_hash):
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET password_hash = %s WHERE id = %s;", (new_password_hash, user_id))
            conn.commit()
    finally:
        db.release_db_connection(conn)

@app.route('/admin/users')
@login_required
@admin_required
def admin_users():
    users = get_all_users()
    return render_template('admin_users.html', users=users)

@app.route('/admin/users/delete/<int:user_id>')
@login_required
@admin_required
def delete_user(user_id):
    if user_id == current_user.id:
        flash("You cannot delete your own account.", 'danger')
        return redirect(url_for('admin_users'))
    
    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE id = %s;", (user_id,))
            conn.commit()
    finally:
        db.release_db_connection(conn)

    flash('User deleted successfully.', 'success')
    return redirect(url_for('admin_users'))

@app.route('/admin/users/promote/<int:user_id>')
@login_required
@admin_required
def promote_user(user_id):
    if user_id == current_user.id:
        flash("You cannot change your own role.", 'danger')
        return redirect(url_for('admin_users'))

    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET role = 'admin' WHERE id = %s;", (user_id,))
            conn.commit()
    finally:
        db.release_db_connection(conn)

    flash('User promoted to admin.', 'success')
    return redirect(url_for('admin_users'))

@app.route('/admin/users/demote/<int:user_id>')
@login_required
@admin_required
def demote_user(user_id):
    if user_id == current_user.id:
        flash("You cannot change your own role.", 'danger')
        return redirect(url_for('admin_users'))

    conn = db.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET role = 'user' WHERE id = %s;", (user_id,))
            conn.commit()
    finally:
        db.release_db_connection(conn)

    flash('User demoted to user.', 'success')
    return redirect(url_for('admin_users'))

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))

@app.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    if request.method == 'POST':
        current_password = request.form.get('current_password')
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')

        if not check_password_hash(current_user.password_hash, current_password):
            flash('Incorrect current password.', 'danger')
            return redirect(url_for('change_password'))

        if new_password != confirm_password:
            flash('New password and confirmation do not match.', 'danger')
            return redirect(url_for('change_password'))

        if len(new_password) < 6:
            flash('New password must be at least 6 characters long.', 'danger')
            return redirect(url_for('change_password'))

        new_password_hash = generate_password_hash(new_password, method='pbkdf2:sha256')
        update_user_password(current_user.id, new_password_hash)
        
        flash('Your password has been changed successfully.', 'success')
        return redirect(url_for('settings'))

    return render_template('change_password.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = get_user_by_username(username)

        if user and check_password_hash(user.password_hash, password):
            if user.totp_secret:
                session['temp_user_id'] = user.id
                return redirect(url_for('verify_2fa'))
            else:
                login_user(user)
                flash('Logged in successfully.', 'success')
                return redirect(url_for('index'))
        else:
            flash('Invalid username or password.', 'danger')
            return redirect(url_for('login'))

    return render_template('login.html')

@app.route('/forgot_password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        username_or_email = request.form.get('username_or_email')
        
        user = get_user_by_username(username_or_email)
        if not user:
            user = get_user_by_email(username_or_email)

        if user and user.email:
            token = s.dumps(user.id, salt='password-reset-salt')
            reset_url = url_for('reset_password', token=token, _external=True)
            msg = Message('Password Reset Request', sender=app.config['MAIL_DEFAULT_SENDER'], recipients=[user.email])
            msg.body = f'To reset your password, visit the following link: {reset_url}'
            try:
                mail.send(msg)
                flash('A password reset link has been sent to your email address.', 'info')
            except Exception as e:
                flash(f'Error sending email: {e}. Please check your mail server configuration.', 'danger')
            return redirect(url_for('login'))
        else:
            flash('Username or email not found, or no email associated with this account.', 'danger')
            return redirect(url_for('forgot_password'))

    return render_template('forgot_password.html')

@app.route('/reset_password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    try:
        user_id = s.loads(token, salt='password-reset-salt', max_age=3600)
    except (SignatureExpired, BadTimeSignature):
        flash('The password reset link is invalid or has expired.', 'danger')
        return redirect(url_for('forgot_password'))
    
    if request.method == 'POST':
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')

        if new_password != confirm_password:
            flash('Passwords do not match.', 'danger')
            return render_template('reset_password.html', token=token)

        if len(new_password) < 6:
            flash('New password must be at least 6 characters long.', 'danger')
            return render_template('reset_password.html', token=token)
        
        hashed_password = generate_password_hash(new_password, method='pbkdf2:sha256')
        update_user_password(user_id, hashed_password)
        flash('Your password has been reset successfully.', 'success')
        return redirect(url_for('login'))

    return render_template('reset_password.html', token=token)

@app.route('/verify_2fa', methods=['GET', 'POST'])
def verify_2fa():
    user_id = session.get('temp_user_id')
    if not user_id:
        return redirect(url_for('login'))
    user = get_user_by_id(user_id)
    if not user:
        return redirect(url_for('login'))

    if request.method == 'POST':
        totp_code = request.form.get('totp_code')
        totp = pyotp.TOTP(user.totp_secret)
        if totp.verify(totp_code):
            login_user(user)
            session.pop('temp_user_id', None)
            return redirect(url_for('index'))
        else:
            flash('Invalid 2FA code.', 'danger')
    return render_template('verify_2fa.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        
        if get_user_by_username(username):
            flash('Username already exists.', 'warning')
            return redirect(url_for('register'))
            
        password_hash = generate_password_hash(password, method='pbkdf2:sha256')
        
        conn = db.get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users;")
                user_count = cur.fetchone()[0]
                role = 'admin' if user_count == 0 else 'user'
                
                cur.execute(
                    "INSERT INTO users (username, email, password_hash, role) VALUES (%s, %s, %s, %s);",
                    (username, email, password_hash, role)
                )
                conn.commit()
        finally:
            db.release_db_connection(conn)
            
        flash('Registration successful! Please log in.', 'success')
        return redirect(url_for('login'))
        
    return render_template('register.html')


# ... (The rest of the routes will be refactored in subsequent steps) ...

@app.route('/', methods=['GET', 'POST'])
@login_required
def index():
    # Load settings dynamically to ensure latest categories and icons are used
    app_settings = settings_manager.get_settings()
    current_expense_categories = app_settings['expense_categories']
    current_category_icons = app_settings['category_icons']
    current_income_categories = app_settings['income_categories']
    current_income_category_icons = app_settings['income_category_icons']
    savings_goals = savings_goals_logic.get_savings_goals()

    if request.method == 'POST':
        transaction_type = request.form.get('type')
        item = request.form.get('item')
        amount = float(request.form.get('amount', 0))
        date = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
        description = request.form.get('description', '')
        savings_goal_id = request.form.get('savings_goal_id')

        if transaction_type == 'income' and item and amount > 0:
            category = request.form.get('category')
            if category in current_income_categories:
                budget_logic.add_transaction('income', category, item, amount, date, description)
        elif transaction_type == 'expense' and item and amount > 0:
            category = request.form.get('category')
            transaction_savings_goal_id = request.form.get('savings_goal_id') if category == 'Goal Savings' else ''

            if category in current_expense_categories:
                if category == 'Goal Savings':
                    if not transaction_savings_goal_id:
                        flash('Please select a savings goal for "Goal Savings" category.', 'danger')
                        return redirect(url_for('index'))
                    budget_logic.add_transaction('expense', category, item, amount, date, description, transaction_savings_goal_id)
                    savings_goals_logic.update_saved_amount(transaction_savings_goal_id, amount)
                else:
                    budget_logic.add_transaction('expense', category, item, amount, date, description, '')
        
        return redirect(url_for('index'))

    all_transactions = budget_logic.get_transactions()
    savings_goals_logic.recalculate_saved_amounts(all_transactions)
    return render_template('index.html', 
                           categories=current_expense_categories, 
                           transactions=all_transactions, 
                           category_icons=current_category_icons, 
                           income_categories=current_income_categories,
                           income_category_icons=current_income_category_icons,
                           savings_goals=savings_goals,
                           today_date=datetime.now().strftime('%Y-%m-%d'))

@app.route('/transactions')
@login_required
def transactions():
    # ... (code remains the same for now)
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    search_query = request.args.get('search_query', '').strip()

    all_transactions = budget_logic.get_transactions()
    
    app_settings = settings_manager.get_settings()
    current_category_icons = app_settings['category_icons']
    income_category_icons = app_settings['income_category_icons']
    
    # ... (rest of the function)
    transactions_to_paginate = all_transactions
    total_transactions = len(transactions_to_paginate)
    total_pages = (total_transactions + per_page - 1) // per_page
    start_index = (page - 1) * per_page
    end_index = start_index + per_page
    paginated_transactions = transactions_to_paginate[start_index:end_index]
    return render_template('transactions.html', 
                           transactions=paginated_transactions, 
                           category_icons=current_category_icons,
                           income_category_icons=income_category_icons,
                           page=page, per_page=per_page, total_pages=total_pages,
                           total_transactions=total_transactions, search_query=search_query)


@app.route('/report')
@login_required
def report():
    # ... (code remains the same for now)
    period = request.args.get('period')
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    report_data = budget_logic.generate_report_data(period=period, start_date_str=start_date_str, end_date_str=end_date_str)
    # ... (rest of the function)
    app_settings = settings_manager.get_settings()
    current_category_icons = app_settings['category_icons']
    income_category_icons = app_settings['income_category_icons']
    return render_template('report.html', report=report_data, current_period=period, category_icons=current_category_icons, income_category_icons=income_category_icons, start_date=report_data['start_date'], end_date=report_data['end_date'])

@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    # ... (code remains the same for now)
    if request.method == 'POST':
        monthly_savings_goal = float(request.form.get('monthly_savings_goal'))
        settings_manager.update_monthly_savings_goal(monthly_savings_goal)
        flash('Settings saved successfully!', 'success')
        return redirect(url_for('settings'))
    current_settings = settings_manager.get_settings()
    return render_template('settings.html', settings=current_settings, current_user=current_user)

# ... (the rest of the settings routes remain for now)
@app.route('/settings/categories', methods=['GET', 'POST'])
@login_required
def manage_categories():
    current_settings = settings_manager.get_settings()
    expense_categories = current_settings['expense_categories']
    category_icons = current_settings['category_icons']

    if request.method == 'POST':
        new_category_name = request.form.get('new_category_name', '').strip()
        new_category_icon = request.form.get('new_category_icon', '').strip()

        if new_category_name:
            # Check for duplicate category name (case-insensitive) - now handled by DB's UNIQUE constraint, but good to keep UI check
            if any(new_category_name.lower() == existing_category.lower() for existing_category in expense_categories):
                flash(f'Category "{new_category_name}" already exists!', 'warning')
                return redirect(url_for('manage_categories'))

            settings_manager.add_expense_category(new_category_name, new_category_icon if new_category_icon else category_icons.get('_default'))
            flash(f'Category "{new_category_name}" added successfully!', 'success')
        else:
            flash('Category name cannot be empty.', 'danger')
        
        return redirect(url_for('manage_categories'))

    return render_template('categories.html', 
                           expense_categories=expense_categories, 
                           category_icons=category_icons,
                           current_settings=current_settings)

@app.route('/delete/<transaction_id>')
@login_required
def delete(transaction_id):
    budget_logic.delete_transaction(transaction_id)
    return redirect(request.referrer or url_for('index'))


@app.route('/edit/<transaction_id>', methods=['GET', 'POST'])
@login_required
def edit(transaction_id):
    # ... (code remains the same for now)
    transaction = budget_logic.get_transaction(transaction_id)
    if request.method == 'POST':
        #...
        pass
    return render_template('edit.html', transaction=transaction)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
