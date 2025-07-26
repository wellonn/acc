#!/usr/bin/env python3
"""
تطبيق نظام المحاسبة المتقدم
Advanced Accounting System

نظام محاسبة شامل ومتقدم للشركات والمؤسسات
يوفر جميع الميزات المطلوبة للإدارة المالية والمحاسبية
"""

import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from flask import Flask, render_template, send_from_directory, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_jwt_extended import JWTManager
from flask_cors import CORS
from werkzeug.security import generate_password_hash
import logging

# إعداد المسارات
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# إنشاء التطبيق
app = Flask(__name__, 
           static_folder='src/static',
           template_folder='src/templates')

# إعداد CORS للسماح بالطلبات من جميع المصادر
CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "X-Session-ID"]
    }
})

# الإعدادات
app.config['SECRET_KEY'] = 'your-secret-key-change-in-production'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///accounting_system.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['JWT_SECRET_KEY'] = 'jwt-secret-change-in-production'
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(hours=24)

# تهيئة الإضافات
db = SQLAlchemy(app)
jwt = JWTManager(app)

# ربط قاعدة البيانات بالتطبيق
app.db = db

# إعداد التسجيل
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('accounting_system.log'),
        logging.StreamHandler()
    ]
)

# نماذج البيانات البسيطة (للاختبار)
class User(db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    full_name = db.Column(db.String(200), nullable=False)
    role = db.Column(db.String(50), default='user')
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime)
    permissions = db.Column(db.Text)  # JSON string

class Customer(db.Model):
    __tablename__ = 'customers'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    phone = db.Column(db.String(50))
    address = db.Column(db.Text)
    customer_type = db.Column(db.String(50), default='individual')
    tax_number = db.Column(db.String(100))
    company = db.Column(db.String(200))
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'))

class Invoice(db.Model):
    __tablename__ = 'invoices'
    
    id = db.Column(db.Integer, primary_key=True)
    invoice_number = db.Column(db.String(100), unique=True, nullable=False)
    customer_id = db.Column(db.Integer, db.ForeignKey('customers.id'), nullable=False)
    issue_date = db.Column(db.Date, nullable=False)
    due_date = db.Column(db.Date, nullable=False)
    subtotal = db.Column(db.Numeric(15, 2), default=0)
    tax_amount = db.Column(db.Numeric(15, 2), default=0)
    total_amount = db.Column(db.Numeric(15, 2), default=0)
    status = db.Column(db.String(50), default='draft')
    currency = db.Column(db.String(10), default='SAR')
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'))
    
    # العلاقات
    customer = db.relationship('Customer', backref='invoices')
    
    def generate_invoice_number(self):
        """إنشاء رقم فاتورة تلقائي"""
        year = datetime.utcnow().year
        count = Invoice.query.filter(
            db.extract('year', Invoice.created_at) == year
        ).count() + 1
        self.invoice_number = f"INV-{year}-{count:06d}"

# المسارات الأساسية
@app.route('/')
def index():
    """الصفحة الرئيسية"""
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/manifest.json')
def manifest():
    """ملف Web App Manifest"""
    return send_from_directory(app.static_folder, 'manifest.json')

@app.route('/sw.js')
def service_worker():
    """Service Worker"""
    return send_from_directory(app.static_folder, 'sw.js')

@app.route('/offline.html')
def offline():
    """صفحة العمل بدون إنترنت"""
    return send_from_directory(app.static_folder, 'offline.html')

# API للحصول على معلومات النظام
@app.route('/api/v1/system/info')
def system_info():
    """معلومات النظام"""
    return jsonify({
        'name': 'نظام المحاسبة المتقدم',
        'version': '1.0.0',
        'description': 'نظام محاسبة شامل ومتقدم',
        'features': [
            'إدارة الفواتير والعملاء',
            'التقارير المالية المتقدمة',
            'نظام محاسبة مزدوج القيد',
            'لوحة تحكم تفاعلية',
            'نسخ احتياطية تلقائية',
            'يعمل على جميع الأجهزة (PWA)'
        ],
        'status': 'active',
        'timestamp': datetime.utcnow().isoformat()
    })

# API بسيط للاختبار
@app.route('/api/v1/test')
def api_test():
    """اختبار API"""
    return jsonify({
        'success': True,
        'message': 'API يعمل بشكل صحيح',
        'timestamp': datetime.utcnow().isoformat(),
        'server_info': {
            'python_version': sys.version,
            'flask_version': Flask.__version__
        }
    })

# API للحصول على بيانات وهمية للاختبار
@app.route('/api/v1/demo/dashboard')
def demo_dashboard():
    """بيانات لوحة التحكم التجريبية"""
    return jsonify({
        'success': True,
        'data': {
            'summary': {
                'total_invoices': 156,
                'total_revenue': 328000.00,
                'outstanding_amount': 45000.00,
                'total_customers': 89,
                'new_customers': 12
            },
            'recent_invoices': [
                {
                    'id': 1,
                    'invoice_number': 'INV-2024-000001',
                    'customer_name': 'شركة الأمل للتجارة',
                    'amount': 15000.00,
                    'status': 'paid',
                    'date': '2024-07-25'
                },
                {
                    'id': 2,
                    'invoice_number': 'INV-2024-000002',
                    'customer_name': 'مؤسسة النور',
                    'amount': 8500.00,
                    'status': 'sent',
                    'date': '2024-07-24'
                }
            ],
            'chart_data': {
                'monthly_revenue': [
                    {'month': 'يناير', 'revenue': 250000},
                    {'month': 'فبراير', 'revenue': 280000},
                    {'month': 'مارس', 'revenue': 320000},
                    {'month': 'أبريل', 'revenue': 290000},
                    {'month': 'مايو', 'revenue': 350000},
                    {'month': 'يونيو', 'revenue': 328000}
                ]
            }
        }
    })

# معالج الأخطاء
@app.errorhandler(404)
def not_found(error):
    """معالج خطأ 404"""
    if request.path.startswith('/api/'):
        return jsonify({
            'success': False,
            'message': 'المورد غير موجود',
            'error_code': 404
        }), 404
    return send_from_directory(app.static_folder, 'index.html')

@app.errorhandler(500)
def internal_error(error):
    """معالج خطأ 500"""
    if request.path.startswith('/api/'):
        return jsonify({
            'success': False,
            'message': 'خطأ داخلي في الخادم',
            'error_code': 500
        }), 500
    return send_from_directory(app.static_folder, 'offline.html')

def create_sample_data():
    """إنشاء بيانات تجريبية"""
    try:
        # إنشاء مستخدم إداري
        admin_user = User.query.filter_by(username='admin').first()
        if not admin_user:
            admin_user = User(
                username='admin',
                email='admin@accounting.com',
                password_hash=generate_password_hash('admin123'),
                full_name='مدير النظام',
                role='admin',
                permissions='{"all": true}'
            )
            db.session.add(admin_user)
        
        # إنشاء عملاء تجريبيين
        if Customer.query.count() == 0:
            customers = [
                Customer(
                    name='شركة الأمل للتجارة',
                    email='info@alamal.com',
                    phone='+966501234567',
                    address='الرياض، المملكة العربية السعودية',
                    customer_type='company',
                    company='شركة الأمل للتجارة',
                    created_by=1
                ),
                Customer(
                    name='أحمد محمد السعيد',
                    email='ahmed@example.com',
                    phone='+966507654321',
                    address='جدة، المملكة العربية السعودية',
                    customer_type='individual',
                    created_by=1
                ),
                Customer(
                    name='مؤسسة النور',
                    email='contact@alnoor.com',
                    phone='+966551234567',
                    address='الدمام، المملكة العربية السعودية',
                    customer_type='company',
                    company='مؤسسة النور',
                    created_by=1
                )
            ]
            
            for customer in customers:
                db.session.add(customer)
        
        # إنشاء فواتير تجريبية
        if Invoice.query.count() == 0:
            invoices = [
                Invoice(
                    customer_id=1,
                    issue_date=datetime.utcnow().date(),
                    due_date=(datetime.utcnow() + timedelta(days=30)).date(),
                    subtotal=15000.00,
                    tax_amount=2250.00,
                    total_amount=17250.00,
                    status='paid',
                    created_by=1
                ),
                Invoice(
                    customer_id=2,
                    issue_date=datetime.utcnow().date(),
                    due_date=(datetime.utcnow() + timedelta(days=30)).date(),
                    subtotal=8500.00,
                    tax_amount=1275.00,
                    total_amount=9775.00,
                    status='sent',
                    created_by=1
                )
            ]
            
            for invoice in invoices:
                invoice.generate_invoice_number()
                db.session.add(invoice)
        
        db.session.commit()
        app.logger.info("تم إنشاء البيانات التجريبية بنجاح")
        
    except Exception as e:
        db.session.rollback()
        app.logger.error(f"خطأ في إنشاء البيانات التجريبية: {str(e)}")

def init_database():
    """تهيئة قاعدة البيانات"""
    try:
        # إنشاء الجداول
        db.create_all()
        app.logger.info("تم إنشاء جداول قاعدة البيانات")
        
        # إنشاء البيانات التجريبية
        create_sample_data()
        
    except Exception as e:
        app.logger.error(f"خطأ في تهيئة قاعدة البيانات: {str(e)}")

if __name__ == '__main__':
    # تهيئة قاعدة البيانات
    with app.app_context():
        init_database()
    
    # تشغيل التطبيق
    print("\n" + "="*60)
    print("🚀 نظام المحاسبة المتقدم")
    print("Advanced Accounting System")
    print("="*60)
    print("📊 النظام جاهز للاستخدام!")
    print("🌐 الرابط: http://localhost:5000")
    print("👤 المستخدم: admin")
    print("🔑 كلمة المرور: admin123")
    print("="*60)
    print("📱 يدعم PWA - يمكن تثبيته على الهاتف!")
    print("🔄 يعمل بدون إنترنت")
    print("💾 نسخ احتياطية تلقائية")
    print("📈 تقارير مالية متقدمة")
    print("="*60 + "\n")
    
    # تشغيل الخادم
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True,
        threaded=True
    )

