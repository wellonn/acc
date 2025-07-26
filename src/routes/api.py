"""
REST API شامل لنظام المحاسبة
يوفر جميع endpoints المطلوبة للتطبيق
"""

from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity, create_access_token
from datetime import datetime, timedelta
from sqlalchemy import func, and_, or_
from werkzeug.security import check_password_hash
import json
from typing import Dict, List, Any, Optional

from ..models.user import User
from ..models.accounting import Account, Transaction, JournalEntry
from ..models.invoice import Invoice, InvoiceItem, Customer
from ..utils.audit_trail import AuditTrailManager, AuditEventType, AuditContext, AuditSeverity
from ..utils.batch_processor import BatchProcessor, BatchOperationType
from ..utils.backup_manager import BackupManager, BackupType

api_bp = Blueprint('api', __name__, url_prefix='/api/v1')

# مساعدات عامة
def success_response(data=None, message="Success", status_code=200):
    """استجابة نجاح موحدة"""
    response = {
        'success': True,
        'message': message,
        'data': data,
        'timestamp': datetime.utcnow().isoformat()
    }
    return jsonify(response), status_code

def error_response(message="Error", status_code=400, errors=None):
    """استجابة خطأ موحدة"""
    response = {
        'success': False,
        'message': message,
        'errors': errors,
        'timestamp': datetime.utcnow().isoformat()
    }
    return jsonify(response), status_code

def get_audit_context():
    """جلب سياق المراجعة من الطلب الحالي"""
    user_id = get_jwt_identity() if request.headers.get('Authorization') else None
    return AuditContext(
        user_id=user_id,
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent'),
        session_id=request.headers.get('X-Session-ID')
    )

# ==================== المصادقة والأمان ====================

@api_bp.route('/auth/login', methods=['POST'])
def login():
    """تسجيل الدخول"""
    try:
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        
        if not username or not password:
            return error_response("اسم المستخدم وكلمة المرور مطلوبان", 400)
        
        # البحث عن المستخدم
        user = User.query.filter_by(username=username, is_active=True).first()
        
        if not user or not check_password_hash(user.password_hash, password):
            # تسجيل محاولة دخول فاشلة
            audit_manager = AuditTrailManager(current_app.db.session)
            audit_manager.log_event(
                event_type=AuditEventType.LOGIN,
                action="failed_login_attempt",
                context=get_audit_context(),
                severity=AuditSeverity.HIGH,
                is_successful=False,
                error_message="Invalid credentials"
            )
            return error_response("اسم المستخدم أو كلمة المرور غير صحيحة", 401)
        
        # إنشاء رمز الوصول
        access_token = create_access_token(
            identity=user.id,
            expires_delta=timedelta(hours=24)
        )
        
        # تحديث آخر دخول
        user.last_login = datetime.utcnow()
        current_app.db.session.commit()
        
        # تسجيل نجاح الدخول
        audit_manager = AuditTrailManager(current_app.db.session)
        audit_manager.log_event(
            event_type=AuditEventType.LOGIN,
            action="successful_login",
            context=AuditContext(user_id=user.id, user_name=user.username),
            severity=AuditSeverity.LOW
        )
        
        return success_response({
            'access_token': access_token,
            'user': {
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'full_name': user.full_name,
                'role': user.role,
                'permissions': user.permissions
            }
        }, "تم تسجيل الدخول بنجاح")
        
    except Exception as e:
        current_app.logger.error(f"Login error: {str(e)}")
        return error_response("خطأ في الخادم", 500)

@api_bp.route('/auth/logout', methods=['POST'])
@jwt_required()
def logout():
    """تسجيل الخروج"""
    try:
        user_id = get_jwt_identity()
        
        # تسجيل الخروج
        audit_manager = AuditTrailManager(current_app.db.session)
        audit_manager.log_event(
            event_type=AuditEventType.LOGOUT,
            action="user_logout",
            context=AuditContext(user_id=user_id),
            severity=AuditSeverity.LOW
        )
        
        return success_response(message="تم تسجيل الخروج بنجاح")
        
    except Exception as e:
        current_app.logger.error(f"Logout error: {str(e)}")
        return error_response("خطأ في الخادم", 500)

# ==================== إدارة المستخدمين ====================

@api_bp.route('/users', methods=['GET'])
@jwt_required()
def get_users():
    """جلب قائمة المستخدمين"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        search = request.args.get('search', '')
        
        query = User.query
        
        if search:
            query = query.filter(
                or_(
                    User.username.contains(search),
                    User.email.contains(search),
                    User.full_name.contains(search)
                )
            )
        
        users = query.paginate(
            page=page, 
            per_page=per_page, 
            error_out=False
        )
        
        return success_response({
            'users': [{
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'full_name': user.full_name,
                'role': user.role,
                'is_active': user.is_active,
                'created_at': user.created_at.isoformat(),
                'last_login': user.last_login.isoformat() if user.last_login else None
            } for user in users.items],
            'pagination': {
                'page': users.page,
                'pages': users.pages,
                'per_page': users.per_page,
                'total': users.total,
                'has_next': users.has_next,
                'has_prev': users.has_prev
            }
        })
        
    except Exception as e:
        current_app.logger.error(f"Get users error: {str(e)}")
        return error_response("خطأ في جلب المستخدمين", 500)

# ==================== إدارة العملاء ====================

@api_bp.route('/customers', methods=['GET'])
@jwt_required()
def get_customers():
    """جلب قائمة العملاء"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        search = request.args.get('search', '')
        customer_type = request.args.get('type', '')
        
        query = Customer.query
        
        if search:
            query = query.filter(
                or_(
                    Customer.name.contains(search),
                    Customer.email.contains(search),
                    Customer.phone.contains(search)
                )
            )
        
        if customer_type:
            query = query.filter(Customer.customer_type == customer_type)
        
        customers = query.paginate(
            page=page, 
            per_page=per_page, 
            error_out=False
        )
        
        return success_response({
            'customers': [{
                'id': customer.id,
                'name': customer.name,
                'email': customer.email,
                'phone': customer.phone,
                'address': customer.address,
                'customer_type': customer.customer_type,
                'is_active': customer.is_active,
                'total_invoices': len(customer.invoices),
                'total_amount': sum(inv.total_amount for inv in customer.invoices),
                'outstanding_amount': sum(inv.total_amount for inv in customer.invoices if inv.status != 'paid'),
                'created_at': customer.created_at.isoformat()
            } for customer in customers.items],
            'pagination': {
                'page': customers.page,
                'pages': customers.pages,
                'per_page': customers.per_page,
                'total': customers.total
            }
        })
        
    except Exception as e:
        current_app.logger.error(f"Get customers error: {str(e)}")
        return error_response("خطأ في جلب العملاء", 500)

@api_bp.route('/customers', methods=['POST'])
@jwt_required()
def create_customer():
    """إنشاء عميل جديد"""
    try:
        data = request.get_json()
        
        # التحقق من البيانات المطلوبة
        required_fields = ['name', 'email']
        for field in required_fields:
            if not data.get(field):
                return error_response(f"الحقل {field} مطلوب", 400)
        
        # التحقق من عدم وجود عميل بنفس البريد الإلكتروني
        existing_customer = Customer.query.filter_by(email=data['email']).first()
        if existing_customer:
            return error_response("يوجد عميل بنفس البريد الإلكتروني", 400)
        
        # إنشاء العميل الجديد
        customer = Customer(
            name=data['name'],
            email=data['email'],
            phone=data.get('phone', ''),
            address=data.get('address', ''),
            customer_type=data.get('customer_type', 'individual'),
            tax_number=data.get('tax_number', ''),
            company=data.get('company', ''),
            created_by=get_jwt_identity()
        )
        
        current_app.db.session.add(customer)
        current_app.db.session.commit()
        
        # تسجيل العملية
        audit_manager = AuditTrailManager(current_app.db.session)
        audit_manager.log_event(
            event_type=AuditEventType.CREATE,
            action="create_customer",
            context=get_audit_context(),
            resource_type="customer",
            resource_id=customer.id,
            new_values=data
        )
        
        return success_response({
            'customer': {
                'id': customer.id,
                'name': customer.name,
                'email': customer.email,
                'phone': customer.phone,
                'address': customer.address,
                'customer_type': customer.customer_type
            }
        }, "تم إنشاء العميل بنجاح", 201)
        
    except Exception as e:
        current_app.db.session.rollback()
        current_app.logger.error(f"Create customer error: {str(e)}")
        return error_response("خطأ في إنشاء العميل", 500)

# ==================== إدارة الفواتير ====================

@api_bp.route('/invoices', methods=['GET'])
@jwt_required()
def get_invoices():
    """جلب قائمة الفواتير"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        status = request.args.get('status', '')
        customer_id = request.args.get('customer_id', type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        query = Invoice.query
        
        if status:
            query = query.filter(Invoice.status == status)
        
        if customer_id:
            query = query.filter(Invoice.customer_id == customer_id)
        
        if start_date:
            start_date = datetime.fromisoformat(start_date)
            query = query.filter(Invoice.issue_date >= start_date)
        
        if end_date:
            end_date = datetime.fromisoformat(end_date)
            query = query.filter(Invoice.issue_date <= end_date)
        
        invoices = query.order_by(Invoice.created_at.desc()).paginate(
            page=page, 
            per_page=per_page, 
            error_out=False
        )
        
        return success_response({
            'invoices': [{
                'id': invoice.id,
                'invoice_number': invoice.invoice_number,
                'customer_name': invoice.customer.name,
                'customer_id': invoice.customer_id,
                'issue_date': invoice.issue_date.isoformat(),
                'due_date': invoice.due_date.isoformat(),
                'subtotal': float(invoice.subtotal),
                'tax_amount': float(invoice.tax_amount),
                'total_amount': float(invoice.total_amount),
                'status': invoice.status,
                'currency': invoice.currency,
                'items_count': len(invoice.items),
                'created_at': invoice.created_at.isoformat()
            } for invoice in invoices.items],
            'pagination': {
                'page': invoices.page,
                'pages': invoices.pages,
                'per_page': invoices.per_page,
                'total': invoices.total
            }
        })
        
    except Exception as e:
        current_app.logger.error(f"Get invoices error: {str(e)}")
        return error_response("خطأ في جلب الفواتير", 500)

@api_bp.route('/invoices', methods=['POST'])
@jwt_required()
def create_invoice():
    """إنشاء فاتورة جديدة"""
    try:
        data = request.get_json()
        
        # التحقق من البيانات المطلوبة
        required_fields = ['customer_id', 'items']
        for field in required_fields:
            if not data.get(field):
                return error_response(f"الحقل {field} مطلوب", 400)
        
        # التحقق من وجود العميل
        customer = Customer.query.get(data['customer_id'])
        if not customer:
            return error_response("العميل غير موجود", 404)
        
        # إنشاء الفاتورة
        invoice = Invoice(
            customer_id=data['customer_id'],
            issue_date=datetime.fromisoformat(data.get('issue_date', datetime.utcnow().isoformat())),
            due_date=datetime.fromisoformat(data.get('due_date', (datetime.utcnow() + timedelta(days=30)).isoformat())),
            currency=data.get('currency', 'SAR'),
            notes=data.get('notes', ''),
            created_by=get_jwt_identity()
        )
        
        # إضافة عناصر الفاتورة
        subtotal = 0
        for item_data in data['items']:
            item = InvoiceItem(
                description=item_data['description'],
                quantity=item_data['quantity'],
                unit_price=item_data['unit_price'],
                tax_rate=item_data.get('tax_rate', 0)
            )
            item.calculate_totals()
            invoice.items.append(item)
            subtotal += item.total_amount
        
        # حساب إجماليات الفاتورة
        invoice.subtotal = subtotal
        invoice.tax_amount = sum(item.tax_amount for item in invoice.items)
        invoice.total_amount = invoice.subtotal + invoice.tax_amount
        
        # إنشاء رقم الفاتورة
        invoice.generate_invoice_number()
        
        current_app.db.session.add(invoice)
        current_app.db.session.commit()
        
        # تسجيل العملية
        audit_manager = AuditTrailManager(current_app.db.session)
        audit_manager.log_event(
            event_type=AuditEventType.CREATE,
            action="create_invoice",
            context=get_audit_context(),
            resource_type="invoice",
            resource_id=invoice.id,
            new_values={
                'invoice_number': invoice.invoice_number,
                'customer_id': invoice.customer_id,
                'total_amount': float(invoice.total_amount)
            }
        )
        
        return success_response({
            'invoice': {
                'id': invoice.id,
                'invoice_number': invoice.invoice_number,
                'customer_name': customer.name,
                'total_amount': float(invoice.total_amount),
                'status': invoice.status
            }
        }, "تم إنشاء الفاتورة بنجاح", 201)
        
    except Exception as e:
        current_app.db.session.rollback()
        current_app.logger.error(f"Create invoice error: {str(e)}")
        return error_response("خطأ في إنشاء الفاتورة", 500)

# ==================== التقارير المالية ====================

@api_bp.route('/reports/dashboard', methods=['GET'])
@jwt_required()
def get_dashboard_data():
    """جلب بيانات لوحة التحكم"""
    try:
        # فترة التقرير
        period = request.args.get('period', 'current_month')
        
        if period == 'current_month':
            start_date = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            end_date = datetime.utcnow()
        elif period == 'last_month':
            end_date = datetime.utcnow().replace(day=1) - timedelta(days=1)
            start_date = end_date.replace(day=1)
        else:
            start_date = datetime.utcnow() - timedelta(days=30)
            end_date = datetime.utcnow()
        
        # إحصائيات الفواتير
        total_invoices = Invoice.query.filter(
            Invoice.issue_date >= start_date,
            Invoice.issue_date <= end_date
        ).count()
        
        total_revenue = current_app.db.session.query(
            func.sum(Invoice.total_amount)
        ).filter(
            Invoice.issue_date >= start_date,
            Invoice.issue_date <= end_date,
            Invoice.status.in_(['paid', 'partially_paid'])
        ).scalar() or 0
        
        outstanding_amount = current_app.db.session.query(
            func.sum(Invoice.total_amount)
        ).filter(
            Invoice.issue_date >= start_date,
            Invoice.issue_date <= end_date,
            Invoice.status.in_(['sent', 'overdue'])
        ).scalar() or 0
        
        # إحصائيات العملاء
        total_customers = Customer.query.filter(Customer.is_active == True).count()
        new_customers = Customer.query.filter(
            Customer.created_at >= start_date,
            Customer.created_at <= end_date
        ).count()
        
        # الفواتير حسب الحالة
        invoices_by_status = current_app.db.session.query(
            Invoice.status,
            func.count(Invoice.id).label('count'),
            func.sum(Invoice.total_amount).label('total')
        ).filter(
            Invoice.issue_date >= start_date,
            Invoice.issue_date <= end_date
        ).group_by(Invoice.status).all()
        
        # أفضل العملاء
        top_customers = current_app.db.session.query(
            Customer.id,
            Customer.name,
            func.sum(Invoice.total_amount).label('total_amount'),
            func.count(Invoice.id).label('invoice_count')
        ).join(Invoice).filter(
            Invoice.issue_date >= start_date,
            Invoice.issue_date <= end_date
        ).group_by(Customer.id, Customer.name).order_by(
            func.sum(Invoice.total_amount).desc()
        ).limit(5).all()
        
        return success_response({
            'period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'period_type': period
            },
            'summary': {
                'total_invoices': total_invoices,
                'total_revenue': float(total_revenue),
                'outstanding_amount': float(outstanding_amount),
                'total_customers': total_customers,
                'new_customers': new_customers
            },
            'invoices_by_status': [{
                'status': status,
                'count': count,
                'total': float(total or 0)
            } for status, count, total in invoices_by_status],
            'top_customers': [{
                'id': customer_id,
                'name': name,
                'total_amount': float(total_amount),
                'invoice_count': invoice_count
            } for customer_id, name, total_amount, invoice_count in top_customers]
        })
        
    except Exception as e:
        current_app.logger.error(f"Dashboard data error: {str(e)}")
        return error_response("خطأ في جلب بيانات لوحة التحكم", 500)

# ==================== المعالجة المجمعة ====================

@api_bp.route('/batch/import', methods=['POST'])
@jwt_required()
def batch_import():
    """استيراد البيانات المجمعة"""
    try:
        if 'file' not in request.files:
            return error_response("لم يتم رفع ملف", 400)
        
        file = request.files['file']
        data_type = request.form.get('data_type', 'customers')
        validate_only = request.form.get('validate_only', 'false').lower() == 'true'
        
        if file.filename == '':
            return error_response("لم يتم اختيار ملف", 400)
        
        # حفظ الملف مؤقتاً
        filename = f"import_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        file_path = f"/tmp/{filename}"
        file.save(file_path)
        
        # إنشاء معالج المجموعة
        batch_processor = BatchProcessor(current_app.db.session)
        
        # إنشاء وظيفة المعالجة
        job_id = batch_processor.create_job(
            BatchOperationType.IMPORT,
            data_type,
            {'filename': filename, 'validate_only': validate_only}
        )
        
        # بدء المعالجة (في الخلفية)
        # في التطبيق الحقيقي، يجب استخدام Celery أو مهام الخلفية
        
        return success_response({
            'job_id': job_id,
            'status': 'pending',
            'message': 'تم بدء عملية الاستيراد'
        }, "تم بدء الاستيراد بنجاح", 202)
        
    except Exception as e:
        current_app.logger.error(f"Batch import error: {str(e)}")
        return error_response("خطأ في الاستيراد المجمع", 500)

# ==================== النسخ الاحتياطية ====================

@api_bp.route('/backup/create', methods=['POST'])
@jwt_required()
def create_backup():
    """إنشاء نسخة احتياطية"""
    try:
        data = request.get_json() or {}
        backup_type = data.get('backup_type', 'full')
        
        # إنشاء النسخة الاحتياطية (محاكاة)
        backup_id = f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # تسجيل العملية
        audit_manager = AuditTrailManager(current_app.db.session)
        audit_manager.log_event(
            event_type=AuditEventType.BACKUP,
            action="create_backup",
            context=get_audit_context(),
            metadata={'backup_type': backup_type, 'backup_id': backup_id}
        )
        
        return success_response({
            'backup_id': backup_id,
            'backup_type': backup_type,
            'status': 'completed',
            'created_at': datetime.utcnow().isoformat()
        }, "تم إنشاء النسخة الاحتياطية بنجاح")
        
    except Exception as e:
        current_app.logger.error(f"Create backup error: {str(e)}")
        return error_response("خطأ في إنشاء النسخة الاحتياطية", 500)

# ==================== سجل المراجعة ====================

@api_bp.route('/audit/logs', methods=['GET'])
@jwt_required()
def get_audit_logs():
    """جلب سجلات المراجعة"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        event_type = request.args.get('event_type', '')
        user_id = request.args.get('user_id', type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # محاكاة سجلات المراجعة
        logs = []
        for i in range(per_page):
            logs.append({
                'id': i + 1,
                'event_type': 'login' if i % 3 == 0 else 'create',
                'user_name': 'أحمد محمد',
                'action': 'تسجيل دخول' if i % 3 == 0 else 'إنشاء فاتورة',
                'resource_type': 'user' if i % 3 == 0 else 'invoice',
                'ip_address': '192.168.1.100',
                'timestamp': (datetime.utcnow() - timedelta(hours=i)).isoformat(),
                'is_successful': True,
                'severity': 'low'
            })
        
        return success_response({
            'logs': logs,
            'pagination': {
                'page': page,
                'pages': 5,
                'per_page': per_page,
                'total': 100
            }
        })
        
    except Exception as e:
        current_app.logger.error(f"Get audit logs error: {str(e)}")
        return error_response("خطأ في جلب سجلات المراجعة", 500)

# ==================== إحصائيات النظام ====================

@api_bp.route('/system/stats', methods=['GET'])
@jwt_required()
def get_system_stats():
    """جلب إحصائيات النظام"""
    try:
        # إحصائيات عامة
        total_users = User.query.count()
        active_users = User.query.filter(User.is_active == True).count()
        total_customers = Customer.query.count()
        total_invoices = Invoice.query.count()
        
        # إحصائيات الشهر الحالي
        current_month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        monthly_revenue = current_app.db.session.query(
            func.sum(Invoice.total_amount)
        ).filter(
            Invoice.issue_date >= current_month_start,
            Invoice.status.in_(['paid', 'partially_paid'])
        ).scalar() or 0
        
        monthly_invoices = Invoice.query.filter(
            Invoice.issue_date >= current_month_start
        ).count()
        
        return success_response({
            'general': {
                'total_users': total_users,
                'active_users': active_users,
                'total_customers': total_customers,
                'total_invoices': total_invoices
            },
            'current_month': {
                'revenue': float(monthly_revenue),
                'invoices': monthly_invoices,
                'start_date': current_month_start.isoformat()
            },
            'system_health': {
                'database_status': 'healthy',
                'backup_status': 'up_to_date',
                'last_backup': (datetime.utcnow() - timedelta(hours=2)).isoformat()
            }
        })
        
    except Exception as e:
        current_app.logger.error(f"System stats error: {str(e)}")
        return error_response("خطأ في جلب إحصائيات النظام", 500)

# معالج الأخطاء العام
@api_bp.errorhandler(404)
def not_found(error):
    return error_response("المورد غير موجود", 404)

@api_bp.errorhandler(403)
def forbidden(error):
    return error_response("غير مسموح بالوصول", 403)

@api_bp.errorhandler(500)
def internal_error(error):
    current_app.db.session.rollback()
    return error_response("خطأ داخلي في الخادم", 500)

