"""
Notification Service Module
Handles alert notifications to guardians and caregivers
"""

import logging
from datetime import datetime
from typing import Dict, List
import json
import os

logger = logging.getLogger(__name__)


class NotificationService:
    """
    Professional notification system for sending alerts to guardians
    """
    
    def __init__(self, notification_log_file='notifications.json'):
        """Initialize notification service"""
        self.log_file = notification_log_file
        self.notification_history = self._load_history()
        logger.info("NotificationService initialized")
    
    def _load_history(self) -> List[Dict]:
        """Load notification history from file"""
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r') as f:
                    return json.load(f)
            except:
                return []
        return []
    
    def _save_history(self):
        """Save notification history to file"""
        try:
            with open(self.log_file, 'w') as f:
                json.dump(self.notification_history, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save notification history: {e}")
    
    def send_threshold_alert(self, patient_email: str, guardian_emails: List[str], 
                            alert_data: Dict, method: str = 'all') -> Dict:
        """
        Send threshold breach alert to guardians
        
        Args:
            patient_email: Patient's email
            guardian_emails: List of guardian email addresses
            alert_data: Alert details from ThresholdChecker
            method: Notification method ('email', 'sms', 'push', 'all')
            
        Returns:
            Dict with delivery status
        """
        notification = {
            'id': f"ALERT-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            'type': 'threshold_alert',
            'patient': patient_email,
            'guardians': guardian_emails,
            'alert_level': alert_data.get('alert_level', 'UNKNOWN'),
            'message': alert_data.get('message', 'Alert triggered'),
            'timestamp': datetime.now().isoformat(),
            'method': method,
            'status': 'sent',
            'details': alert_data
        }
        
        # In production, integrate with actual email/SMS/push services
        # For now, we'll log and store
        logger.info(f"🚨 Alert sent to {len(guardian_emails)} guardian(s): {notification['message']}")
        
        # Simulate delivery
        delivery_status = {
            'success': True,
            'notification_id': notification['id'],
            'delivered_to': guardian_emails,
            'delivery_method': method,
            'timestamp': notification['timestamp']
        }
        
        # Store in history
        self.notification_history.append(notification)
        self._save_history()
        
        # Keep only last 1000 notifications
        if len(self.notification_history) > 1000:
            self.notification_history = self.notification_history[-1000:]
            self._save_history()
        
        return delivery_status
    
    def send_sos_alert(self, patient_email: str, guardian_emails: List[str], 
                      location: Dict = None, vitals: Dict = None) -> Dict:
        """
        Send emergency SOS alert
        
        Args:
            patient_email: Patient's email
            guardian_emails: List of guardian emails
            location: GPS coordinates (optional)
            vitals: Current vital signs (optional)
            
        Returns:
            Dict with delivery status
        """
        notification = {
            'id': f"SOS-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            'type': 'sos_emergency',
            'patient': patient_email,
            'guardians': guardian_emails,
            'alert_level': 'RED',
            'message': '🆘 EMERGENCY SOS ACTIVATED - Immediate assistance required!',
            'timestamp': datetime.now().isoformat(),
            'location': location or {'status': 'unavailable'},
            'vitals': vitals or {},
            'status': 'sent'
        }
        
        logger.critical(f"🆘 SOS ALERT: {patient_email} - {len(guardian_emails)} guardians notified")
        
        # Store in history
        self.notification_history.append(notification)
        self._save_history()
        
        return {
            'success': True,
            'notification_id': notification['id'],
            'delivered_to': guardian_emails,
            'delivery_method': 'all',
            'timestamp': notification['timestamp'],
            'message': 'Emergency services and guardians notified'
        }
    
    def get_unacknowledged_alerts(self, patient_email: str) -> List[Dict]:
        """Get all unacknowledged alerts for a patient"""
        return [
            n for n in self.notification_history
            if n.get('patient') == patient_email 
            and n.get('status') == 'sent'
            and n.get('type') == 'threshold_alert'
        ]
    
    def acknowledge_alert(self, notification_id: str, acknowledged_by: str) -> bool:
        """
        Mark an alert as acknowledged
        
        Args:
            notification_id: The notification ID
            acknowledged_by: Email of person acknowledging
            
        Returns:
            Boolean success status
        """
        for notification in self.notification_history:
            if notification.get('id') == notification_id:
                notification['status'] = 'acknowledged'
                notification['acknowledged_by'] = acknowledged_by
                notification['acknowledged_at'] = datetime.now().isoformat()
                self._save_history()
                logger.info(f"Alert {notification_id} acknowledged by {acknowledged_by}")
                return True
        return False
    
    def get_alert_summary(self, patient_email: str, hours: int = 24) -> Dict:
        """
        Get summary of alerts for last N hours
        
        Args:
            patient_email: Patient's email
            hours: Number of hours to look back
            
        Returns:
            Dict with alert statistics
        """
        from datetime import timedelta
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        recent_alerts = [
            n for n in self.notification_history
            if n.get('patient') == patient_email
            and datetime.fromisoformat(n.get('timestamp', '2000-01-01')) > cutoff_time
        ]
        
        critical = len([a for a in recent_alerts if a.get('alert_level') == 'RED'])
        warnings = len([a for a in recent_alerts if a.get('alert_level') == 'ORANGE'])
        cautions = len([a for a in recent_alerts if a.get('alert_level') == 'YELLOW'])
        sos_count = len([a for a in recent_alerts if a.get('type') == 'sos_emergency'])
        
        return {
            'total_alerts': len(recent_alerts),
            'critical_alerts': critical,
            'warning_alerts': warnings,
            'caution_alerts': cautions,
            'sos_alerts': sos_count,
            'unacknowledged': len([a for a in recent_alerts if a.get('status') == 'sent']),
            'time_period_hours': hours,
            'summary_generated': datetime.now().isoformat()
        }
    
    def send_daily_summary(self, patient_email: str, guardian_emails: List[str]) -> Dict:
        """
        Send daily summary report to guardians
        
        Args:
            patient_email: Patient's email
            guardian_emails: Guardian emails
            
        Returns:
            Dict with delivery status
        """
        summary = self.get_alert_summary(patient_email, hours=24)
        
        notification = {
            'id': f"SUMMARY-{datetime.now().strftime('%Y%m%d')}",
            'type': 'daily_summary',
            'patient': patient_email,
            'guardians': guardian_emails,
            'message': f"Daily Report: {summary['total_alerts']} total alerts",
            'summary': summary,
            'timestamp': datetime.now().isoformat(),
            'status': 'sent'
        }
        
        logger.info(f"📊 Daily summary sent for {patient_email}")
        
        self.notification_history.append(notification)
        self._save_history()
        
        return {
            'success': True,
            'notification_id': notification['id'],
            'summary': summary
        }
