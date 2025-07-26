"""
نظام معالجة البيانات المجمعة (Batch Processing)
يتيح استيراد وتصدير ومعالجة كميات كبيرة من البيانات بكفاءة
"""

import csv
import json
import pandas as pd
import openpyxl
from typing import Dict, List, Any, Optional, Callable, Generator
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
import io
import zipfile
from pathlib import Path

class BatchOperationType(Enum):
    """أنواع العمليات المجمعة"""
    IMPORT = "import"
    EXPORT = "export"
    UPDATE = "update"
    DELETE = "delete"
    VALIDATE = "validate"
    TRANSFORM = "transform"

class BatchStatus(Enum):
    """حالات المعالجة المجمعة"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PARTIAL_SUCCESS = "partial_success"

@dataclass
class BatchResult:
    """نتيجة العملية المجمعة"""
    total_records: int
    processed_records: int
    successful_records: int
    failed_records: int
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    execution_time: float
    status: BatchStatus

@dataclass
class BatchJob:
    """وظيفة المعالجة المجمعة"""
    id: str
    operation_type: BatchOperationType
    data_type: str  # نوع البيانات (invoices, customers, etc.)
    status: BatchStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: float = 0.0
    result: Optional[BatchResult] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class DataValidator:
    """مُتحقق من صحة البيانات"""
    
    def __init__(self):
        self.validation_rules = {
            'invoices': self._validate_invoice,
            'customers': self._validate_customer,
            'products': self._validate_product,
            'transactions': self._validate_transaction
        }
    
    def validate_record(self, data_type: str, record: Dict[str, Any]) -> List[str]:
        """التحقق من صحة سجل واحد"""
        validator = self.validation_rules.get(data_type)
        if not validator:
            return [f"No validator found for data type: {data_type}"]
        
        return validator(record)
    
    def _validate_invoice(self, record: Dict[str, Any]) -> List[str]:
        """التحقق من صحة بيانات الفاتورة"""
        errors = []
        
        # التحقق من الحقول المطلوبة
        required_fields = ['customer_name', 'amount', 'date', 'items']
        for field in required_fields:
            if not record.get(field):
                errors.append(f"Missing required field: {field}")
        
        # التحقق من صحة المبلغ
        try:
            amount = float(record.get('amount', 0))
            if amount <= 0:
                errors.append("Amount must be greater than 0")
        except (ValueError, TypeError):
            errors.append("Invalid amount format")
        
        # التحقق من صحة التاريخ
        try:
            if record.get('date'):
                datetime.strptime(record['date'], '%Y-%m-%d')
        except ValueError:
            errors.append("Invalid date format. Use YYYY-MM-DD")
        
        # التحقق من العناصر
        items = record.get('items', [])
        if not isinstance(items, list) or len(items) == 0:
            errors.append("Invoice must have at least one item")
        
        return errors
    
    def _validate_customer(self, record: Dict[str, Any]) -> List[str]:
        """التحقق من صحة بيانات العميل"""
        errors = []
        
        required_fields = ['name', 'email']
        for field in required_fields:
            if not record.get(field):
                errors.append(f"Missing required field: {field}")
        
        # التحقق من صحة البريد الإلكتروني
        email = record.get('email', '')
        if email and '@' not in email:
            errors.append("Invalid email format")
        
        # التحقق من رقم الهاتف
        phone = record.get('phone', '')
        if phone and not phone.replace('+', '').replace('-', '').replace(' ', '').isdigit():
            errors.append("Invalid phone number format")
        
        return errors
    
    def _validate_product(self, record: Dict[str, Any]) -> List[str]:
        """التحقق من صحة بيانات المنتج"""
        errors = []
        
        required_fields = ['name', 'price']
        for field in required_fields:
            if not record.get(field):
                errors.append(f"Missing required field: {field}")
        
        # التحقق من صحة السعر
        try:
            price = float(record.get('price', 0))
            if price < 0:
                errors.append("Price cannot be negative")
        except (ValueError, TypeError):
            errors.append("Invalid price format")
        
        return errors
    
    def _validate_transaction(self, record: Dict[str, Any]) -> List[str]:
        """التحقق من صحة بيانات المعاملة"""
        errors = []
        
        required_fields = ['account', 'amount', 'date', 'type']
        for field in required_fields:
            if not record.get(field):
                errors.append(f"Missing required field: {field}")
        
        # التحقق من نوع المعاملة
        transaction_type = record.get('type', '').lower()
        if transaction_type not in ['debit', 'credit']:
            errors.append("Transaction type must be 'debit' or 'credit'")
        
        return errors

class DataTransformer:
    """محول البيانات"""
    
    def __init__(self):
        self.transformers = {
            'invoices': self._transform_invoice,
            'customers': self._transform_customer,
            'products': self._transform_product,
            'transactions': self._transform_transaction
        }
    
    def transform_record(self, data_type: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """تحويل سجل واحد"""
        transformer = self.transformers.get(data_type)
        if not transformer:
            return record
        
        return transformer(record)
    
    def _transform_invoice(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """تحويل بيانات الفاتورة"""
        transformed = record.copy()
        
        # تحويل التاريخ
        if 'date' in transformed:
            try:
                date_obj = datetime.strptime(transformed['date'], '%Y-%m-%d')
                transformed['date'] = date_obj
            except ValueError:
                pass
        
        # تحويل المبلغ
        if 'amount' in transformed:
            try:
                transformed['amount'] = float(transformed['amount'])
            except (ValueError, TypeError):
                transformed['amount'] = 0.0
        
        # إضافة حقول محسوبة
        transformed['created_at'] = datetime.utcnow()
        transformed['status'] = 'draft'
        
        return transformed
    
    def _transform_customer(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """تحويل بيانات العميل"""
        transformed = record.copy()
        
        # تنظيف البريد الإلكتروني
        if 'email' in transformed:
            transformed['email'] = transformed['email'].lower().strip()
        
        # تنظيف رقم الهاتف
        if 'phone' in transformed:
            phone = transformed['phone'].replace('-', '').replace(' ', '').replace('(', '').replace(')', '')
            transformed['phone'] = phone
        
        # إضافة حقول افتراضية
        transformed['created_at'] = datetime.utcnow()
        transformed['is_active'] = True
        
        return transformed
    
    def _transform_product(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """تحويل بيانات المنتج"""
        transformed = record.copy()
        
        # تحويل السعر
        if 'price' in transformed:
            try:
                transformed['price'] = float(transformed['price'])
            except (ValueError, TypeError):
                transformed['price'] = 0.0
        
        # إضافة حقول افتراضية
        transformed['created_at'] = datetime.utcnow()
        transformed['is_active'] = True
        
        return transformed
    
    def _transform_transaction(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """تحويل بيانات المعاملة"""
        transformed = record.copy()
        
        # تحويل التاريخ
        if 'date' in transformed:
            try:
                date_obj = datetime.strptime(transformed['date'], '%Y-%m-%d')
                transformed['date'] = date_obj
            except ValueError:
                pass
        
        # تحويل المبلغ
        if 'amount' in transformed:
            try:
                transformed['amount'] = float(transformed['amount'])
            except (ValueError, TypeError):
                transformed['amount'] = 0.0
        
        # تنظيف نوع المعاملة
        if 'type' in transformed:
            transformed['type'] = transformed['type'].lower()
        
        return transformed

class BatchProcessor:
    """معالج البيانات المجمعة"""
    
    def __init__(self, db_session, logger=None):
        self.db_session = db_session
        self.logger = logger or logging.getLogger(__name__)
        self.validator = DataValidator()
        self.transformer = DataTransformer()
        self.jobs: Dict[str, BatchJob] = {}
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    def create_job(
        self,
        operation_type: BatchOperationType,
        data_type: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """إنشاء وظيفة معالجة جديدة"""
        job_id = f"{operation_type.value}_{data_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        job = BatchJob(
            id=job_id,
            operation_type=operation_type,
            data_type=data_type,
            status=BatchStatus.PENDING,
            created_at=datetime.utcnow(),
            metadata=metadata or {}
        )
        
        self.jobs[job_id] = job
        return job_id
    
    def get_job_status(self, job_id: str) -> Optional[BatchJob]:
        """جلب حالة الوظيفة"""
        return self.jobs.get(job_id)
    
    def cancel_job(self, job_id: str) -> bool:
        """إلغاء وظيفة"""
        job = self.jobs.get(job_id)
        if job and job.status in [BatchStatus.PENDING, BatchStatus.PROCESSING]:
            job.status = BatchStatus.CANCELLED
            return True
        return False
    
    async def import_from_file(
        self,
        job_id: str,
        file_path: str,
        data_type: str,
        file_format: str = 'csv',
        chunk_size: int = 1000,
        validate_only: bool = False
    ) -> BatchResult:
        """استيراد البيانات من ملف"""
        
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        
        job.status = BatchStatus.PROCESSING
        job.started_at = datetime.utcnow()
        
        start_time = datetime.utcnow()
        errors = []
        warnings = []
        total_records = 0
        processed_records = 0
        successful_records = 0
        
        try:
            # قراءة البيانات من الملف
            records = self._read_file(file_path, file_format)
            total_records = len(records)
            
            # معالجة البيانات في مجموعات
            for i in range(0, total_records, chunk_size):
                chunk = records[i:i + chunk_size]
                
                for record_index, record in enumerate(chunk):
                    try:
                        # التحقق من صحة البيانات
                        validation_errors = self.validator.validate_record(data_type, record)
                        if validation_errors:
                            errors.append({
                                'record_index': i + record_index,
                                'record': record,
                                'errors': validation_errors
                            })
                            continue
                        
                        # تحويل البيانات
                        transformed_record = self.transformer.transform_record(data_type, record)
                        
                        # حفظ البيانات (إذا لم يكن التحقق فقط)
                        if not validate_only:
                            await self._save_record(data_type, transformed_record)
                        
                        successful_records += 1
                        
                    except Exception as e:
                        errors.append({
                            'record_index': i + record_index,
                            'record': record,
                            'errors': [str(e)]
                        })
                    
                    processed_records += 1
                
                # تحديث التقدم
                job.progress = (processed_records / total_records) * 100
                
                # إيقاف مؤقت لتجنب إرهاق النظام
                await asyncio.sleep(0.01)
            
            # تحديد حالة النهاية
            if successful_records == total_records:
                job.status = BatchStatus.COMPLETED
            elif successful_records > 0:
                job.status = BatchStatus.PARTIAL_SUCCESS
            else:
                job.status = BatchStatus.FAILED
            
        except Exception as e:
            job.status = BatchStatus.FAILED
            job.error_message = str(e)
            self.logger.error(f"Batch import failed: {str(e)}")
        
        finally:
            job.completed_at = datetime.utcnow()
            execution_time = (job.completed_at - start_time).total_seconds()
            
            job.result = BatchResult(
                total_records=total_records,
                processed_records=processed_records,
                successful_records=successful_records,
                failed_records=len(errors),
                errors=errors,
                warnings=warnings,
                execution_time=execution_time,
                status=job.status
            )
        
        return job.result
    
    async def export_to_file(
        self,
        job_id: str,
        data_type: str,
        file_path: str,
        file_format: str = 'csv',
        filters: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000
    ) -> BatchResult:
        """تصدير البيانات إلى ملف"""
        
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        
        job.status = BatchStatus.PROCESSING
        job.started_at = datetime.utcnow()
        
        start_time = datetime.utcnow()
        total_records = 0
        processed_records = 0
        successful_records = 0
        errors = []
        
        try:
            # جلب البيانات من قاعدة البيانات
            records = await self._fetch_records(data_type, filters, chunk_size)
            total_records = len(records)
            
            # كتابة البيانات إلى الملف
            await self._write_file(file_path, file_format, records)
            
            successful_records = total_records
            processed_records = total_records
            job.status = BatchStatus.COMPLETED
            
        except Exception as e:
            job.status = BatchStatus.FAILED
            job.error_message = str(e)
            errors.append({'error': str(e)})
            self.logger.error(f"Batch export failed: {str(e)}")
        
        finally:
            job.completed_at = datetime.utcnow()
            execution_time = (job.completed_at - start_time).total_seconds()
            
            job.result = BatchResult(
                total_records=total_records,
                processed_records=processed_records,
                successful_records=successful_records,
                failed_records=len(errors),
                errors=errors,
                warnings=[],
                execution_time=execution_time,
                status=job.status
            )
        
        return job.result
    
    def _read_file(self, file_path: str, file_format: str) -> List[Dict[str, Any]]:
        """قراءة البيانات من ملف"""
        
        if file_format.lower() == 'csv':
            return self._read_csv(file_path)
        elif file_format.lower() in ['xlsx', 'xls']:
            return self._read_excel(file_path)
        elif file_format.lower() == 'json':
            return self._read_json(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    
    def _read_csv(self, file_path: str) -> List[Dict[str, Any]]:
        """قراءة ملف CSV"""
        records = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                records.append(dict(row))
        return records
    
    def _read_excel(self, file_path: str) -> List[Dict[str, Any]]:
        """قراءة ملف Excel"""
        df = pd.read_excel(file_path)
        return df.to_dict('records')
    
    def _read_json(self, file_path: str) -> List[Dict[str, Any]]:
        """قراءة ملف JSON"""
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            if isinstance(data, list):
                return data
            else:
                return [data]
    
    async def _write_file(self, file_path: str, file_format: str, records: List[Dict[str, Any]]):
        """كتابة البيانات إلى ملف"""
        
        if file_format.lower() == 'csv':
            await self._write_csv(file_path, records)
        elif file_format.lower() in ['xlsx', 'xls']:
            await self._write_excel(file_path, records)
        elif file_format.lower() == 'json':
            await self._write_json(file_path, records)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    
    async def _write_csv(self, file_path: str, records: List[Dict[str, Any]]):
        """كتابة ملف CSV"""
        if not records:
            return
        
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
    
    async def _write_excel(self, file_path: str, records: List[Dict[str, Any]]):
        """كتابة ملف Excel"""
        df = pd.DataFrame(records)
        df.to_excel(file_path, index=False)
    
    async def _write_json(self, file_path: str, records: List[Dict[str, Any]]):
        """كتابة ملف JSON"""
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(records, file, ensure_ascii=False, indent=2, default=str)
    
    async def _save_record(self, data_type: str, record: Dict[str, Any]):
        """حفظ سجل في قاعدة البيانات"""
        # يجب تخصيص هذه الوظيفة حسب نماذج البيانات
        pass
    
    async def _fetch_records(
        self,
        data_type: str,
        filters: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000
    ) -> List[Dict[str, Any]]:
        """جلب السجلات من قاعدة البيانات"""
        # يجب تخصيص هذه الوظيفة حسب نماذج البيانات
        return []
    
    def create_template(self, data_type: str, file_format: str = 'csv') -> str:
        """إنشاء قالب لاستيراد البيانات"""
        
        templates = {
            'invoices': {
                'customer_name': 'اسم العميل',
                'amount': 'المبلغ',
                'date': 'التاريخ (YYYY-MM-DD)',
                'description': 'الوصف',
                'items': 'العناصر (JSON)'
            },
            'customers': {
                'name': 'الاسم',
                'email': 'البريد الإلكتروني',
                'phone': 'رقم الهاتف',
                'address': 'العنوان',
                'company': 'الشركة'
            },
            'products': {
                'name': 'اسم المنتج',
                'price': 'السعر',
                'category': 'الفئة',
                'description': 'الوصف',
                'sku': 'رمز المنتج'
            }
        }
        
        template_data = templates.get(data_type, {})
        if not template_data:
            raise ValueError(f"No template available for data type: {data_type}")
        
        # إنشاء ملف القالب
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        template_path = f"/tmp/{data_type}_template_{timestamp}.{file_format}"
        
        if file_format.lower() == 'csv':
            with open(template_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=template_data.keys())
                writer.writeheader()
                # إضافة صف مثال
                example_row = {key: value for key, value in template_data.items()}
                writer.writerow(example_row)
        
        return template_path

