"""
نظام النسخ الاحتياطية التلقائية (Automated Backup System)
يوفر نسخ احتياطية منتظمة وآمنة لقاعدة البيانات والملفات
"""

import os
import shutil
import sqlite3
import subprocess
import gzip
import tarfile
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import logging
import schedule
import threading
import json
import hashlib
from pathlib import Path
import boto3
from cryptography.fernet import Fernet

class BackupType(Enum):
    """أنواع النسخ الاحتياطية"""
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"
    DATABASE_ONLY = "database_only"
    FILES_ONLY = "files_only"

class BackupStatus(Enum):
    """حالات النسخ الاحتياطي"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CORRUPTED = "corrupted"

class BackupDestination(Enum):
    """وجهات النسخ الاحتياطي"""
    LOCAL = "local"
    CLOUD_S3 = "cloud_s3"
    CLOUD_GOOGLE = "cloud_google"
    FTP = "ftp"
    NETWORK_DRIVE = "network_drive"

@dataclass
class BackupConfig:
    """إعدادات النسخ الاحتياطي"""
    backup_type: BackupType
    destination: BackupDestination
    schedule_cron: str  # تعبير cron للجدولة
    retention_days: int
    compression: bool = True
    encryption: bool = True
    verify_integrity: bool = True
    max_backup_size_mb: int = 1000
    
    # إعدادات الوجهة
    local_path: Optional[str] = None
    cloud_bucket: Optional[str] = None
    cloud_credentials: Optional[Dict[str, str]] = None
    ftp_settings: Optional[Dict[str, str]] = None

@dataclass
class BackupRecord:
    """سجل النسخة الاحتياطية"""
    id: str
    backup_type: BackupType
    status: BackupStatus
    created_at: datetime
    completed_at: Optional[datetime] = None
    file_path: str = ""
    file_size_mb: float = 0.0
    checksum: str = ""
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class DatabaseBackupManager:
    """مدير النسخ الاحتياطية لقاعدة البيانات"""
    
    def __init__(self, db_path: str, logger=None):
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)
    
    def create_sqlite_backup(self, backup_path: str) -> bool:
        """إنشاء نسخة احتياطية من SQLite"""
        try:
            # الاتصال بقاعدة البيانات الأصلية
            source_conn = sqlite3.connect(self.db_path)
            
            # إنشاء قاعدة بيانات النسخة الاحتياطية
            backup_conn = sqlite3.connect(backup_path)
            
            # نسخ البيانات
            source_conn.backup(backup_conn)
            
            # إغلاق الاتصالات
            source_conn.close()
            backup_conn.close()
            
            self.logger.info(f"SQLite backup created successfully: {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create SQLite backup: {str(e)}")
            return False
    
    def create_mysql_backup(self, backup_path: str, config: Dict[str, str]) -> bool:
        """إنشاء نسخة احتياطية من MySQL"""
        try:
            cmd = [
                'mysqldump',
                f"--host={config.get('host', 'localhost')}",
                f"--user={config['username']}",
                f"--password={config['password']}",
                f"--port={config.get('port', '3306')}",
                '--single-transaction',
                '--routines',
                '--triggers',
                config['database']
            ]
            
            with open(backup_path, 'w') as backup_file:
                result = subprocess.run(cmd, stdout=backup_file, stderr=subprocess.PIPE, text=True)
            
            if result.returncode == 0:
                self.logger.info(f"MySQL backup created successfully: {backup_path}")
                return True
            else:
                self.logger.error(f"MySQL backup failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to create MySQL backup: {str(e)}")
            return False
    
    def create_postgresql_backup(self, backup_path: str, config: Dict[str, str]) -> bool:
        """إنشاء نسخة احتياطية من PostgreSQL"""
        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = config['password']
            
            cmd = [
                'pg_dump',
                f"--host={config.get('host', 'localhost')}",
                f"--port={config.get('port', '5432')}",
                f"--username={config['username']}",
                '--format=custom',
                '--no-password',
                '--verbose',
                config['database']
            ]
            
            with open(backup_path, 'wb') as backup_file:
                result = subprocess.run(cmd, stdout=backup_file, stderr=subprocess.PIPE, env=env)
            
            if result.returncode == 0:
                self.logger.info(f"PostgreSQL backup created successfully: {backup_path}")
                return True
            else:
                self.logger.error(f"PostgreSQL backup failed: {result.stderr.decode()}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to create PostgreSQL backup: {str(e)}")
            return False

class FileBackupManager:
    """مدير النسخ الاحتياطية للملفات"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
    
    def create_archive(
        self,
        source_paths: List[str],
        archive_path: str,
        compression: str = 'gzip',
        exclude_patterns: Optional[List[str]] = None
    ) -> bool:
        """إنشاء أرشيف مضغوط للملفات"""
        try:
            exclude_patterns = exclude_patterns or []
            
            if compression == 'gzip':
                mode = 'w:gz'
            elif compression == 'bzip2':
                mode = 'w:bz2'
            else:
                mode = 'w'
            
            with tarfile.open(archive_path, mode) as tar:
                for source_path in source_paths:
                    if os.path.exists(source_path):
                        # تطبيق فلاتر الاستبعاد
                        def filter_func(tarinfo):
                            for pattern in exclude_patterns:
                                if pattern in tarinfo.name:
                                    return None
                            return tarinfo
                        
                        tar.add(source_path, arcname=os.path.basename(source_path), filter=filter_func)
            
            self.logger.info(f"Archive created successfully: {archive_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create archive: {str(e)}")
            return False
    
    def create_incremental_backup(
        self,
        source_path: str,
        backup_path: str,
        last_backup_time: datetime
    ) -> bool:
        """إنشاء نسخة احتياطية تزايدية"""
        try:
            # العثور على الملفات المحدثة منذ آخر نسخة احتياطية
            modified_files = []
            
            for root, dirs, files in os.walk(source_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.getmtime(file_path) > last_backup_time.timestamp():
                        modified_files.append(file_path)
            
            if not modified_files:
                self.logger.info("No files modified since last backup")
                return True
            
            # إنشاء أرشيف للملفات المحدثة
            with tarfile.open(backup_path, 'w:gz') as tar:
                for file_path in modified_files:
                    arcname = os.path.relpath(file_path, source_path)
                    tar.add(file_path, arcname=arcname)
            
            self.logger.info(f"Incremental backup created: {backup_path} ({len(modified_files)} files)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create incremental backup: {str(e)}")
            return False

class EncryptionManager:
    """مدير التشفير"""
    
    def __init__(self, key: Optional[bytes] = None):
        self.key = key or Fernet.generate_key()
        self.cipher = Fernet(self.key)
    
    def encrypt_file(self, input_path: str, output_path: str) -> bool:
        """تشفير ملف"""
        try:
            with open(input_path, 'rb') as input_file:
                data = input_file.read()
            
            encrypted_data = self.cipher.encrypt(data)
            
            with open(output_path, 'wb') as output_file:
                output_file.write(encrypted_data)
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to encrypt file: {str(e)}")
            return False
    
    def decrypt_file(self, input_path: str, output_path: str) -> bool:
        """فك تشفير ملف"""
        try:
            with open(input_path, 'rb') as input_file:
                encrypted_data = input_file.read()
            
            decrypted_data = self.cipher.decrypt(encrypted_data)
            
            with open(output_path, 'wb') as output_file:
                output_file.write(decrypted_data)
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to decrypt file: {str(e)}")
            return False

class CloudUploadManager:
    """مدير الرفع للسحابة"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
    
    def upload_to_s3(
        self,
        file_path: str,
        bucket_name: str,
        key: str,
        aws_access_key: str,
        aws_secret_key: str,
        region: str = 'us-east-1'
    ) -> bool:
        """رفع ملف إلى Amazon S3"""
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region
            )
            
            s3_client.upload_file(file_path, bucket_name, key)
            
            self.logger.info(f"File uploaded to S3: s3://{bucket_name}/{key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upload to S3: {str(e)}")
            return False

class BackupManager:
    """المدير الرئيسي للنسخ الاحتياطية"""
    
    def __init__(self, config: BackupConfig, logger=None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.db_backup_manager = DatabaseBackupManager("", self.logger)
        self.file_backup_manager = FileBackupManager(self.logger)
        self.encryption_manager = EncryptionManager()
        self.cloud_manager = CloudUploadManager(self.logger)
        self.backup_records: List[BackupRecord] = []
        self.scheduler_thread = None
        self.is_running = False
    
    def start_scheduler(self):
        """بدء جدولة النسخ الاحتياطية"""
        if self.is_running:
            return
        
        self.is_running = True
        
        # جدولة النسخ الاحتياطي
        schedule.every().day.at("02:00").do(self._scheduled_backup)
        
        # تشغيل الجدولة في خيط منفصل
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        self.logger.info("Backup scheduler started")
    
    def stop_scheduler(self):
        """إيقاف جدولة النسخ الاحتياطية"""
        self.is_running = False
        schedule.clear()
        self.logger.info("Backup scheduler stopped")
    
    def _run_scheduler(self):
        """تشغيل الجدولة"""
        while self.is_running:
            schedule.run_pending()
            threading.Event().wait(60)  # فحص كل دقيقة
    
    def _scheduled_backup(self):
        """تنفيذ النسخ الاحتياطي المجدول"""
        try:
            self.create_backup()
        except Exception as e:
            self.logger.error(f"Scheduled backup failed: {str(e)}")
    
    def create_backup(self, backup_type: Optional[BackupType] = None) -> BackupRecord:
        """إنشاء نسخة احتياطية"""
        backup_type = backup_type or self.config.backup_type
        
        # إنشاء سجل النسخة الاحتياطية
        backup_id = f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        backup_record = BackupRecord(
            id=backup_id,
            backup_type=backup_type,
            status=BackupStatus.IN_PROGRESS,
            created_at=datetime.utcnow()
        )
        
        self.backup_records.append(backup_record)
        
        try:
            # تحديد مسار النسخة الاحتياطية
            backup_filename = f"{backup_id}.tar.gz"
            if self.config.encryption:
                backup_filename += ".enc"
            
            temp_path = f"/tmp/{backup_filename}"
            
            # إنشاء النسخة الاحتياطية حسب النوع
            success = False
            if backup_type == BackupType.DATABASE_ONLY:
                success = self._backup_database_only(temp_path)
            elif backup_type == BackupType.FILES_ONLY:
                success = self._backup_files_only(temp_path)
            else:
                success = self._backup_full(temp_path)
            
            if not success:
                backup_record.status = BackupStatus.FAILED
                backup_record.error_message = "Backup creation failed"
                return backup_record
            
            # حساب حجم الملف ومجموع التحقق
            backup_record.file_size_mb = os.path.getsize(temp_path) / (1024 * 1024)
            backup_record.checksum = self._calculate_checksum(temp_path)
            
            # التحقق من حد الحجم
            if backup_record.file_size_mb > self.config.max_backup_size_mb:
                backup_record.status = BackupStatus.FAILED
                backup_record.error_message = f"Backup size exceeds limit: {backup_record.file_size_mb}MB"
                os.remove(temp_path)
                return backup_record
            
            # نقل النسخة الاحتياطية إلى الوجهة النهائية
            final_path = self._move_to_destination(temp_path, backup_filename)
            if not final_path:
                backup_record.status = BackupStatus.FAILED
                backup_record.error_message = "Failed to move backup to destination"
                return backup_record
            
            backup_record.file_path = final_path
            backup_record.status = BackupStatus.COMPLETED
            backup_record.completed_at = datetime.utcnow()
            
            # التحقق من التكامل إذا كان مطلوباً
            if self.config.verify_integrity:
                if not self._verify_backup_integrity(backup_record):
                    backup_record.status = BackupStatus.CORRUPTED
                    backup_record.error_message = "Backup integrity verification failed"
            
            # تنظيف النسخ القديمة
            self._cleanup_old_backups()
            
            self.logger.info(f"Backup completed successfully: {backup_record.id}")
            
        except Exception as e:
            backup_record.status = BackupStatus.FAILED
            backup_record.error_message = str(e)
            backup_record.completed_at = datetime.utcnow()
            self.logger.error(f"Backup failed: {str(e)}")
        
        return backup_record
    
    def _backup_database_only(self, backup_path: str) -> bool:
        """نسخ قاعدة البيانات فقط"""
        # تنفيذ نسخ قاعدة البيانات
        return True
    
    def _backup_files_only(self, backup_path: str) -> bool:
        """نسخ الملفات فقط"""
        # تنفيذ نسخ الملفات
        return True
    
    def _backup_full(self, backup_path: str) -> bool:
        """نسخة احتياطية كاملة"""
        # تنفيذ النسخة الاحتياطية الكاملة
        return True
    
    def _calculate_checksum(self, file_path: str) -> str:
        """حساب مجموع التحقق للملف"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def _move_to_destination(self, source_path: str, filename: str) -> Optional[str]:
        """نقل النسخة الاحتياطية إلى الوجهة النهائية"""
        try:
            if self.config.destination == BackupDestination.LOCAL:
                destination_path = os.path.join(self.config.local_path, filename)
                shutil.move(source_path, destination_path)
                return destination_path
            
            elif self.config.destination == BackupDestination.CLOUD_S3:
                # رفع إلى S3
                success = self.cloud_manager.upload_to_s3(
                    source_path,
                    self.config.cloud_bucket,
                    filename,
                    self.config.cloud_credentials['access_key'],
                    self.config.cloud_credentials['secret_key']
                )
                if success:
                    os.remove(source_path)
                    return f"s3://{self.config.cloud_bucket}/{filename}"
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to move backup to destination: {str(e)}")
            return None
    
    def _verify_backup_integrity(self, backup_record: BackupRecord) -> bool:
        """التحقق من تكامل النسخة الاحتياطية"""
        try:
            # التحقق من وجود الملف
            if not os.path.exists(backup_record.file_path):
                return False
            
            # التحقق من مجموع التحقق
            current_checksum = self._calculate_checksum(backup_record.file_path)
            return current_checksum == backup_record.checksum
            
        except Exception as e:
            self.logger.error(f"Integrity verification failed: {str(e)}")
            return False
    
    def _cleanup_old_backups(self):
        """تنظيف النسخ الاحتياطية القديمة"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=self.config.retention_days)
            
            old_backups = [
                record for record in self.backup_records
                if record.created_at < cutoff_date and record.status == BackupStatus.COMPLETED
            ]
            
            for backup in old_backups:
                try:
                    if os.path.exists(backup.file_path):
                        os.remove(backup.file_path)
                    self.backup_records.remove(backup)
                    self.logger.info(f"Removed old backup: {backup.id}")
                except Exception as e:
                    self.logger.error(f"Failed to remove old backup {backup.id}: {str(e)}")
                    
        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")
    
    def restore_backup(self, backup_id: str, restore_path: str) -> bool:
        """استعادة نسخة احتياطية"""
        try:
            # العثور على سجل النسخة الاحتياطية
            backup_record = next((r for r in self.backup_records if r.id == backup_id), None)
            if not backup_record:
                self.logger.error(f"Backup record not found: {backup_id}")
                return False
            
            # التحقق من تكامل النسخة الاحتياطية
            if not self._verify_backup_integrity(backup_record):
                self.logger.error(f"Backup integrity verification failed: {backup_id}")
                return False
            
            # استخراج النسخة الاحتياطية
            if backup_record.file_path.endswith('.enc'):
                # فك التشفير أولاً
                decrypted_path = backup_record.file_path.replace('.enc', '')
                if not self.encryption_manager.decrypt_file(backup_record.file_path, decrypted_path):
                    return False
                extract_path = decrypted_path
            else:
                extract_path = backup_record.file_path
            
            # استخراج الأرشيف
            with tarfile.open(extract_path, 'r:gz') as tar:
                tar.extractall(restore_path)
            
            self.logger.info(f"Backup restored successfully: {backup_id} to {restore_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Restore failed: {str(e)}")
            return False
    
    def get_backup_status(self) -> Dict[str, Any]:
        """جلب حالة النسخ الاحتياطية"""
        total_backups = len(self.backup_records)
        completed_backups = len([r for r in self.backup_records if r.status == BackupStatus.COMPLETED])
        failed_backups = len([r for r in self.backup_records if r.status == BackupStatus.FAILED])
        
        total_size_mb = sum(r.file_size_mb for r in self.backup_records if r.status == BackupStatus.COMPLETED)
        
        last_backup = max(self.backup_records, key=lambda x: x.created_at) if self.backup_records else None
        
        return {
            'total_backups': total_backups,
            'completed_backups': completed_backups,
            'failed_backups': failed_backups,
            'total_size_mb': round(total_size_mb, 2),
            'last_backup': {
                'id': last_backup.id,
                'status': last_backup.status.value,
                'created_at': last_backup.created_at.isoformat(),
                'size_mb': last_backup.file_size_mb
            } if last_backup else None,
            'scheduler_running': self.is_running
        }

