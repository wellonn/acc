"""
نظام سجل المراجعة المتقدم (Audit Trail)
يسجل جميع العمليات والتغييرات في النظام لأغراض المراجعة والامتثال
"""

import json
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
from dataclasses import dataclass, asdict
from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import logging

Base = declarative_base()

class AuditEventType(Enum):
    """أنواع أحداث المراجعة"""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    LOGIN = "login"
    LOGOUT = "logout"
    VIEW = "view"
    EXPORT = "export"
    IMPORT = "import"
    PAYMENT = "payment"
    INVOICE_SENT = "invoice_sent"
    BACKUP = "backup"
    RESTORE = "restore"
    SYSTEM_CONFIG = "system_config"
    USER_PERMISSION = "user_permission"

class AuditSeverity(Enum):
    """مستويات خطورة أحداث المراجعة"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class AuditContext:
    """سياق حدث المراجعة"""
    user_id: Optional[int] = None
    user_name: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None

class AuditLog(Base):
    """نموذج سجل المراجعة"""
    __tablename__ = 'audit_logs'
    
    id = Column(Integer, primary_key=True)
    event_type = Column(String(50), nullable=False)
    severity = Column(String(20), nullable=False, default='medium')
    
    # معلومات المستخدم
    user_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    user_name = Column(String(100), nullable=True)
    
    # معلومات الجلسة
    session_id = Column(String(100), nullable=True)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    
    # تفاصيل الحدث
    resource_type = Column(String(50), nullable=True)  # نوع المورد (invoice, customer, etc.)
    resource_id = Column(String(50), nullable=True)    # معرف المورد
    action = Column(String(100), nullable=False)       # الإجراء المنفذ
    description = Column(Text, nullable=True)          # وصف الحدث
    
    # البيانات
    old_values = Column(Text, nullable=True)           # القيم القديمة (JSON)
    new_values = Column(Text, nullable=True)           # القيم الجديدة (JSON)
    metadata = Column(Text, nullable=True)             # بيانات إضافية (JSON)
    
    # التوقيت
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # التحقق من التكامل
    checksum = Column(String(64), nullable=True)       # مجموع التحقق
    
    # حالة الحدث
    is_successful = Column(Boolean, default=True)
    error_message = Column(Text, nullable=True)
    
    # العلاقات
    user = relationship("User", back_populates="audit_logs")

class AuditTrailManager:
    """مدير سجل المراجعة"""
    
    def __init__(self, db_session, logger=None):
        self.db_session = db_session
        self.logger = logger or logging.getLogger(__name__)
    
    def log_event(
        self,
        event_type: AuditEventType,
        action: str,
        context: AuditContext,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        old_values: Optional[Dict[str, Any]] = None,
        new_values: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
        severity: AuditSeverity = AuditSeverity.MEDIUM,
        metadata: Optional[Dict[str, Any]] = None,
        is_successful: bool = True,
        error_message: Optional[str] = None
    ) -> AuditLog:
        """تسجيل حدث مراجعة جديد"""
        
        try:
            # إنشاء سجل المراجعة
            audit_log = AuditLog(
                event_type=event_type.value,
                severity=severity.value,
                user_id=context.user_id,
                user_name=context.user_name,
                session_id=context.session_id,
                ip_address=context.ip_address,
                user_agent=context.user_agent,
                resource_type=resource_type,
                resource_id=str(resource_id) if resource_id else None,
                action=action,
                description=description,
                old_values=json.dumps(old_values, ensure_ascii=False) if old_values else None,
                new_values=json.dumps(new_values, ensure_ascii=False) if new_values else None,
                metadata=json.dumps(metadata, ensure_ascii=False) if metadata else None,
                is_successful=is_successful,
                error_message=error_message
            )
            
            # حساب مجموع التحقق
            audit_log.checksum = self._calculate_checksum(audit_log)
            
            # حفظ السجل
            self.db_session.add(audit_log)
            self.db_session.commit()
            
            self.logger.info(f"Audit event logged: {event_type.value} - {action}")
            return audit_log
            
        except Exception as e:
            self.logger.error(f"Failed to log audit event: {str(e)}")
            self.db_session.rollback()
            raise
    
    def _calculate_checksum(self, audit_log: AuditLog) -> str:
        """حساب مجموع التحقق للسجل"""
        data = {
            'event_type': audit_log.event_type,
            'user_id': audit_log.user_id,
            'action': audit_log.action,
            'resource_type': audit_log.resource_type,
            'resource_id': audit_log.resource_id,
            'timestamp': audit_log.timestamp.isoformat() if audit_log.timestamp else None,
            'old_values': audit_log.old_values,
            'new_values': audit_log.new_values
        }
        
        data_string = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(data_string.encode('utf-8')).hexdigest()
    
    def verify_integrity(self, audit_log: AuditLog) -> bool:
        """التحقق من تكامل سجل المراجعة"""
        if not audit_log.checksum:
            return False
        
        calculated_checksum = self._calculate_checksum(audit_log)
        return calculated_checksum == audit_log.checksum
    
    def get_user_activity(
        self,
        user_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        event_types: Optional[List[AuditEventType]] = None,
        limit: int = 100
    ) -> List[AuditLog]:
        """جلب نشاط مستخدم محدد"""
        
        query = self.db_session.query(AuditLog).filter(AuditLog.user_id == user_id)
        
        if start_date:
            query = query.filter(AuditLog.timestamp >= start_date)
        
        if end_date:
            query = query.filter(AuditLog.timestamp <= end_date)
        
        if event_types:
            event_type_values = [et.value for et in event_types]
            query = query.filter(AuditLog.event_type.in_(event_type_values))
        
        return query.order_by(AuditLog.timestamp.desc()).limit(limit).all()
    
    def get_resource_history(
        self,
        resource_type: str,
        resource_id: str,
        limit: int = 50
    ) -> List[AuditLog]:
        """جلب تاريخ مورد محدد"""
        
        return self.db_session.query(AuditLog).filter(
            AuditLog.resource_type == resource_type,
            AuditLog.resource_id == str(resource_id)
        ).order_by(AuditLog.timestamp.desc()).limit(limit).all()
    
    def get_security_events(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        severity: Optional[AuditSeverity] = None,
        limit: int = 100
    ) -> List[AuditLog]:
        """جلب الأحداث الأمنية"""
        
        security_events = [
            AuditEventType.LOGIN.value,
            AuditEventType.LOGOUT.value,
            AuditEventType.USER_PERMISSION.value,
            AuditEventType.SYSTEM_CONFIG.value
        ]
        
        query = self.db_session.query(AuditLog).filter(
            AuditLog.event_type.in_(security_events)
        )
        
        if start_date:
            query = query.filter(AuditLog.timestamp >= start_date)
        
        if end_date:
            query = query.filter(AuditLog.timestamp <= end_date)
        
        if severity:
            query = query.filter(AuditLog.severity == severity.value)
        
        return query.order_by(AuditLog.timestamp.desc()).limit(limit).all()
    
    def get_failed_operations(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[AuditLog]:
        """جلب العمليات الفاشلة"""
        
        query = self.db_session.query(AuditLog).filter(
            AuditLog.is_successful == False
        )
        
        if start_date:
            query = query.filter(AuditLog.timestamp >= start_date)
        
        if end_date:
            query = query.filter(AuditLog.timestamp <= end_date)
        
        return query.order_by(AuditLog.timestamp.desc()).limit(limit).all()
    
    def generate_audit_report(
        self,
        start_date: datetime,
        end_date: datetime,
        user_id: Optional[int] = None,
        event_types: Optional[List[AuditEventType]] = None
    ) -> Dict[str, Any]:
        """إنشاء تقرير مراجعة شامل"""
        
        query = self.db_session.query(AuditLog).filter(
            AuditLog.timestamp >= start_date,
            AuditLog.timestamp <= end_date
        )
        
        if user_id:
            query = query.filter(AuditLog.user_id == user_id)
        
        if event_types:
            event_type_values = [et.value for et in event_types]
            query = query.filter(AuditLog.event_type.in_(event_type_values))
        
        logs = query.all()
        
        # إحصائيات التقرير
        total_events = len(logs)
        successful_events = len([log for log in logs if log.is_successful])
        failed_events = total_events - successful_events
        
        # تجميع الأحداث حسب النوع
        events_by_type = {}
        for log in logs:
            if log.event_type not in events_by_type:
                events_by_type[log.event_type] = 0
            events_by_type[log.event_type] += 1
        
        # تجميع الأحداث حسب المستخدم
        events_by_user = {}
        for log in logs:
            user_key = log.user_name or f"User {log.user_id}" or "Unknown"
            if user_key not in events_by_user:
                events_by_user[user_key] = 0
            events_by_user[user_key] += 1
        
        # تجميع الأحداث حسب الخطورة
        events_by_severity = {}
        for log in logs:
            if log.severity not in events_by_severity:
                events_by_severity[log.severity] = 0
            events_by_severity[log.severity] += 1
        
        return {
            'period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            },
            'summary': {
                'total_events': total_events,
                'successful_events': successful_events,
                'failed_events': failed_events,
                'success_rate': (successful_events / total_events * 100) if total_events > 0 else 0
            },
            'events_by_type': events_by_type,
            'events_by_user': events_by_user,
            'events_by_severity': events_by_severity,
            'recent_events': [
                {
                    'id': log.id,
                    'timestamp': log.timestamp.isoformat(),
                    'event_type': log.event_type,
                    'user_name': log.user_name,
                    'action': log.action,
                    'resource_type': log.resource_type,
                    'resource_id': log.resource_id,
                    'is_successful': log.is_successful,
                    'severity': log.severity
                }
                for log in sorted(logs, key=lambda x: x.timestamp, reverse=True)[:10]
            ]
        }
    
    def cleanup_old_logs(self, days_to_keep: int = 365) -> int:
        """تنظيف السجلات القديمة"""
        
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        deleted_count = self.db_session.query(AuditLog).filter(
            AuditLog.timestamp < cutoff_date
        ).delete()
        
        self.db_session.commit()
        
        self.logger.info(f"Cleaned up {deleted_count} old audit logs")
        return deleted_count

# مساعدات للاستخدام السهل
class AuditDecorator:
    """مُزخرف لتسجيل العمليات تلقائياً"""
    
    def __init__(self, audit_manager: AuditTrailManager):
        self.audit_manager = audit_manager
    
    def log_operation(
        self,
        event_type: AuditEventType,
        resource_type: Optional[str] = None,
        action: Optional[str] = None,
        severity: AuditSeverity = AuditSeverity.MEDIUM
    ):
        """مُزخرف لتسجيل العمليات"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                # استخراج السياق من الطلب (يجب تخصيصه حسب إطار العمل)
                context = AuditContext()  # يجب ملؤه من الطلب الحالي
                
                operation_action = action or func.__name__
                
                try:
                    # تنفيذ الوظيفة
                    result = func(*args, **kwargs)
                    
                    # تسجيل النجاح
                    self.audit_manager.log_event(
                        event_type=event_type,
                        action=operation_action,
                        context=context,
                        resource_type=resource_type,
                        severity=severity,
                        is_successful=True
                    )
                    
                    return result
                    
                except Exception as e:
                    # تسجيل الفشل
                    self.audit_manager.log_event(
                        event_type=event_type,
                        action=operation_action,
                        context=context,
                        resource_type=resource_type,
                        severity=AuditSeverity.HIGH,
                        is_successful=False,
                        error_message=str(e)
                    )
                    
                    raise
            
            return wrapper
        return decorator

# مثال على الاستخدام
def create_audit_context_from_request(request) -> AuditContext:
    """إنشاء سياق المراجعة من الطلب"""
    return AuditContext(
        user_id=getattr(request, 'user_id', None),
        user_name=getattr(request, 'user_name', None),
        ip_address=request.remote_addr if hasattr(request, 'remote_addr') else None,
        user_agent=request.headers.get('User-Agent') if hasattr(request, 'headers') else None,
        session_id=getattr(request, 'session_id', None)
    )

