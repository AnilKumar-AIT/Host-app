"""
Threshold Checker Module
Monitors limb angles and brain-movement correlation for dangerous deviations
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ThresholdChecker:
    """
    Professional threshold monitoring system for brain-limb angles
    """
    
    # Standard biomechanical thresholds (in degrees)
    SAFE_RANGES = {
        'right_arm': {'min': 70, 'max': 110, 'optimal': 85},
        'left_arm': {'min': 70, 'max': 110, 'optimal': 82},
        'right_leg': {'min': 155, 'max': 175, 'optimal': 168},
        'left_leg': {'min': 155, 'max': 175, 'optimal': 165},
    }
    
    # Brain-movement correlation thresholds
    BRAIN_SYNC_THRESHOLDS = {
        'critical': 0.65,
        'warning': 0.75,
        'normal': 0.80
    }
    
    # Alert level configurations
    ALERT_LEVELS = {
        'RED': {
            'priority': 1,
            'label': 'CRITICAL',
            'color': '#C0392B',
            'icon': 'fa-circle-exclamation',
            'notify_immediately': True
        },
        'ORANGE': {
            'priority': 2,
            'label': 'WARNING',
            'color': '#D4A017',
            'icon': 'fa-triangle-exclamation',
            'notify_immediately': False
        },
        'YELLOW': {
            'priority': 3,
            'label': 'CAUTION',
            'color': '#F5C842',
            'icon': 'fa-circle-info',
            'notify_immediately': False
        }
    }
    
    def __init__(self, user_baseline: Dict = None):
        """
        Initialize threshold checker with optional custom baseline
        
        Args:
            user_baseline: Custom baseline angles for the user (optional)
        """
        self.baseline = user_baseline if user_baseline else self.SAFE_RANGES
        logger.info("ThresholdChecker initialized")
    
    def check_limb_angle(self, limb: str, angle: float) -> Dict:
        """
        Check if a limb angle is within safe thresholds
        
        Args:
            limb: Limb identifier (right_arm, left_arm, right_leg, left_leg)
            angle: Current angle measurement in degrees
            
        Returns:
            Dict containing status, alert_level, deviation, and message
        """
        if limb not in self.baseline:
            logger.error(f"Invalid limb identifier: {limb}")
            return {
                'status': 'error',
                'message': f'Invalid limb identifier: {limb}'
            }
        
        thresholds = self.baseline[limb]
        min_safe = thresholds['min']
        max_safe = thresholds['max']
        optimal = thresholds['optimal']
        
        # Calculate deviation from optimal
        deviation = abs(angle - optimal)
        deviation_from_range = 0
        
        # Determine if outside safe range
        if angle < min_safe:
            deviation_from_range = min_safe - angle
            direction = 'below'
        elif angle > max_safe:
            deviation_from_range = angle - max_safe
            direction = 'above'
        else:
            direction = 'within'
        
        # Determine alert level
        alert_level = self._determine_alert_level(deviation_from_range)
        
        # Build result
        result = {
            'limb': limb,
            'angle': round(angle, 1),
            'status': 'safe' if alert_level is None else 'alert',
            'alert_level': alert_level,
            'deviation': round(deviation, 1),
            'deviation_from_range': round(deviation_from_range, 1),
            'direction': direction,
            'safe_range': f"{min_safe}-{max_safe}°",
            'optimal': optimal,
            'timestamp': datetime.now().isoformat(),
            'message': self._generate_message(limb, angle, alert_level, direction, deviation_from_range)
        }
        
        # Add alert configuration if alert exists
        if alert_level:
            result['alert_config'] = self.ALERT_LEVELS[alert_level]
        
        logger.info(f"Checked {limb}: {angle}° - Status: {result['status']}")
        return result
    
    def _determine_alert_level(self, deviation_from_range: float) -> str:
        """
        Determine alert level based on deviation from safe range
        
        Args:
            deviation_from_range: Degrees outside safe range (0 if within range)
            
        Returns:
            Alert level string ('RED', 'ORANGE', 'YELLOW') or None if safe
        """
        if deviation_from_range >= 15:
            return 'RED'
        elif deviation_from_range >= 10:
            return 'ORANGE'
        elif deviation_from_range >= 5:
            return 'YELLOW'
        return None
    
    def _generate_message(self, limb: str, angle: float, alert_level: str, 
                         direction: str, deviation: float) -> str:
        """Generate human-readable alert message"""
        limb_display = limb.replace('_', ' ').title()
        
        if alert_level == 'RED':
            return (f"🚨 CRITICAL: {limb_display} angle ({angle}°) is FAR OUTSIDE safe range "
                   f"({deviation}° {direction} threshold). Immediate intervention required!")
        elif alert_level == 'ORANGE':
            return (f"⚠️ WARNING: {limb_display} angle ({angle}°) is approaching danger zone "
                   f"({deviation}° {direction} threshold). Monitor closely.")
        elif alert_level == 'YELLOW':
            return (f"⚡ CAUTION: {limb_display} angle ({angle}°) is slightly outside normal range "
                   f"({deviation}° {direction} threshold).")
        else:
            return f"✓ {limb_display} angle ({angle}°) is within safe range."
    
    def check_brain_sync(self, correlation_value: float) -> Dict:
        """
        Check brain-movement synchronization level
        
        Args:
            correlation_value: Brain-movement correlation coefficient (0.0 - 1.0)
            
        Returns:
            Dict containing status and recommendations
        """
        result = {
            'value': round(correlation_value, 2),
            'timestamp': datetime.now().isoformat()
        }
        
        if correlation_value < self.BRAIN_SYNC_THRESHOLDS['critical']:
            result['status'] = 'critical'
            result['alert_level'] = 'RED'
            result['message'] = (f"🚨 CRITICAL: Brain-movement sync at {correlation_value:.2f} - "
                               f"significantly below safe threshold. High fall risk!")
            result['recommendation'] = "Immediate rest required. Notify guardian and clinician."
        elif correlation_value < self.BRAIN_SYNC_THRESHOLDS['warning']:
            result['status'] = 'warning'
            result['alert_level'] = 'ORANGE'
            result['message'] = (f"⚠️ WARNING: Brain-movement sync at {correlation_value:.2f} - "
                               f"below optimal range.")
            result['recommendation'] = "Reduce physical activity. Take breaks. Monitor closely."
        elif correlation_value < self.BRAIN_SYNC_THRESHOLDS['normal']:
            result['status'] = 'caution'
            result['alert_level'] = 'YELLOW'
            result['message'] = f"⚡ Moderate brain-movement sync at {correlation_value:.2f}."
            result['recommendation'] = "Continue current activities. Maintain awareness."
        else:
            result['status'] = 'excellent'
            result['alert_level'] = None
            result['message'] = f"✓ Excellent brain-movement sync at {correlation_value:.2f}!"
            result['recommendation'] = "All systems optimal. Continue normal activities."
        
        if result['alert_level']:
            result['alert_config'] = self.ALERT_LEVELS[result['alert_level']]
        
        logger.info(f"Brain sync check: {correlation_value:.2f} - Status: {result['status']}")
        return result
    
    def check_all_limbs(self, angles: Dict[str, float]) -> List[Dict]:
        """
        Check all limbs at once and return comprehensive report
        
        Args:
            angles: Dict mapping limb names to current angles
            
        Returns:
            List of check results for each limb
        """
        results = []
        alerts = []
        
        for limb, angle in angles.items():
            result = self.check_limb_angle(limb, angle)
            results.append(result)
            
            if result['status'] == 'alert':
                alerts.append(result)
        
        # Sort alerts by priority
        alerts.sort(key=lambda x: self.ALERT_LEVELS[x['alert_level']]['priority'])
        
        logger.info(f"Checked all limbs - {len(alerts)} alerts generated")
        
        return {
            'results': results,
            'alerts': alerts,
            'alert_count': len(alerts),
            'critical_count': len([a for a in alerts if a['alert_level'] == 'RED']),
            'warning_count': len([a for a in alerts if a['alert_level'] == 'ORANGE']),
            'caution_count': len([a for a in alerts if a['alert_level'] == 'YELLOW']),
            'timestamp': datetime.now().isoformat()
        }
    
    def calculate_risk_score(self, angles: Dict[str, float], 
                            brain_sync: float, 
                            posture_score: int) -> Dict:
        """
        Calculate composite fall risk score
        
        Args:
            angles: Current limb angles
            brain_sync: Brain-movement correlation
            posture_score: Posture stability score (0-100)
            
        Returns:
            Dict with risk score and breakdown
        """
        # Check all limbs
        limb_check = self.check_all_limbs(angles)
        
        # Calculate limb risk factor
        limb_risk = 0
        for result in limb_check['results']:
            if result['alert_level'] == 'RED':
                limb_risk += 30
            elif result['alert_level'] == 'ORANGE':
                limb_risk += 15
            elif result['alert_level'] == 'YELLOW':
                limb_risk += 5
        
        limb_risk = min(limb_risk, 40)  # Cap at 40%
        
        # Brain sync risk factor
        brain_check = self.check_brain_sync(brain_sync)
        if brain_check['status'] == 'critical':
            brain_risk = 40
        elif brain_check['status'] == 'warning':
            brain_risk = 25
        elif brain_check['status'] == 'caution':
            brain_risk = 10
        else:
            brain_risk = 0
        
        # Posture risk factor
        posture_risk = max(0, 100 - posture_score) * 0.2
        
        # Calculate total risk
        total_risk = min(100, limb_risk + brain_risk + posture_risk)
        
        # Determine risk level
        if total_risk < 20:
            risk_level = 'Low'
            risk_color = '#2D8C6E'
        elif total_risk < 35:
            risk_level = 'Moderate'
            risk_color = '#D4A017'
        else:
            risk_level = 'High'
            risk_color = '#C0392B'
        
        return {
            'total_risk': round(total_risk, 1),
            'risk_level': risk_level,
            'risk_color': risk_color,
            'breakdown': {
                'limb_risk': round(limb_risk, 1),
                'brain_risk': round(brain_risk, 1),
                'posture_risk': round(posture_risk, 1)
            },
            'limb_alerts': limb_check['alert_count'],
            'brain_status': brain_check['status'],
            'recommendations': self._generate_risk_recommendations(risk_level, limb_check, brain_check),
            'timestamp': datetime.now().isoformat()
        }
    
    def _generate_risk_recommendations(self, risk_level: str, 
                                      limb_check: Dict, 
                                      brain_check: Dict) -> List[str]:
        """Generate actionable recommendations based on risk level"""
        recommendations = []
        
        if risk_level == 'High':
            recommendations.append("🚨 IMMEDIATE ACTION: Contact guardian/caregiver now")
            recommendations.append("🛑 STOP all physical activities immediately")
            recommendations.append("💺 Sit or lie down in a safe location")
            recommendations.append("📞 Keep phone nearby for emergency calls")
        elif risk_level == 'Moderate':
            recommendations.append("⚠️ Reduce activity level and movement speed")
            recommendations.append("👀 Ensure caregiver is aware of your status")
            recommendations.append("🚶 Move slowly and deliberately")
            recommendations.append("🪑 Use support (walls, railings, walker)")
        else:
            recommendations.append("✓ Continue normal activities with standard precautions")
            recommendations.append("💪 Maintain regular exercise routine")
            recommendations.append("📊 Keep monitoring as scheduled")
        
        # Add specific recommendations based on alerts
        if limb_check['critical_count'] > 0:
            recommendations.insert(0, f"⚠️ {limb_check['critical_count']} limb(s) in critical range - seek medical attention")
        
        if brain_check['status'] in ['critical', 'warning']:
            recommendations.insert(0, "🧠 Low brain-movement sync detected - rest recommended")
        
        return recommendations
