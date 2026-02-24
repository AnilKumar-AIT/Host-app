from flask import Flask, render_template, request, redirect, url_for, session, jsonify
import json, os, hashlib, secrets
from datetime import datetime, timedelta
import random, math
import logging

# Import our professional utilities
from utils.threshold_checker import ThresholdChecker
from utils.notification_service import NotificationService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

# Initialize professional services
threshold_checker = ThresholdChecker()
notification_service = NotificationService()

logger.info("FallVision Application Started")

USERS_FILE = 'users.json'

# ── Auth helpers ──────────────────────────────────────────────────────────────
def load_users():
    if not os.path.exists(USERS_FILE):
        return {}
    with open(USERS_FILE) as f:
        return json.load(f)

def save_users(users):
    with open(USERS_FILE, 'w') as f:
        json.dump(users, f, indent=2)

def hash_password(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def current_user():
    return session.get('user')

def login_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user():
            return redirect(url_for('login'))
        return fn(*args, **kwargs)
    return wrapper

# ── Mock data generator ───────────────────────────────────────────────────────
def seed(uid):
    random.seed(hash(uid) % 10000)

def gen_angle(base, noise=12):
    return round(base + random.uniform(-noise, noise), 1)

def gen_kpis(uid):
    seed(uid)
    mobility   = random.randint(62, 94)
    posture    = random.randint(58, 97)
    fall_risk  = random.randint(8, 45)
    brain_corr = round(random.uniform(0.61, 0.96), 2)
    return dict(mobility=mobility, posture=posture, fall_risk=fall_risk, brain_corr=brain_corr)

def gen_limb_angles(uid, limb):
    seed(uid + limb)
    bases = {'right_arm':85,'left_arm':82,'right_leg':168,'left_leg':165}
    base = bases.get(limb, 90)
    return [gen_angle(base) for _ in range(8)]

def gen_history(uid, days=30):
    seed(uid)
    records = []
    base_date = datetime.now() - timedelta(days=days)
    for i in range(days):
        d = base_date + timedelta(days=i)
        records.append({
            'date': d.strftime('%Y-%m-%d'),
            'mobility': random.randint(55, 95),
            'posture': random.randint(50, 98),
            'fall_risk': random.randint(5, 55),
            'steps': random.randint(1800, 9400),
            'brain_corr': round(random.uniform(0.55, 0.97), 2),
            'right_arm': gen_angle(85),
            'left_arm': gen_angle(82),
            'right_leg': gen_angle(168),
            'left_leg': gen_angle(165),
        })
    return records

def gen_alerts(uid):
    seed(uid + 'alerts')
    severity = ['Low','Medium','High']
    alerts = []
    for i in range(5):
        s = random.choice(severity)
        alerts.append({
            'id': i+1,
            'severity': s,
            'time': (datetime.now() - timedelta(hours=random.randint(1,72))).strftime('%b %d, %H:%M'),
            'message': random.choice([
                'Unusual gait pattern detected during morning walk',
                'Postural sway exceeded safe threshold',
                'Rapid deceleration event logged — possible near-fall',
                'Left leg extension angle below optimal range',
                'Brain-movement synchrony dropped below baseline',
                'Prolonged sedentary period with elevated stiffness index',
            ]),
        })
    return alerts

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route('/')
def landing():
    if current_user():
        return redirect(url_for('dashboard'))
    return render_template('landing.html')

@app.route('/login', methods=['GET','POST'])
def login():
    error = None
    if request.method == 'POST':
        email = request.form['email'].strip().lower()
        pw    = request.form['password']
        users = load_users()
        if email in users and users[email]['password'] == hash_password(pw):
            session['user'] = {'email': email, **{k:v for k,v in users[email].items() if k!='password'}}
            return redirect(url_for('dashboard'))
        error = 'Invalid email or password.'
    return render_template('login.html', error=error)

@app.route('/signup', methods=['GET','POST'])
def signup():
    error = None
    if request.method == 'POST':
        name  = request.form['name'].strip()
        email = request.form['email'].strip().lower()
        pw    = request.form['password']
        role  = request.form.get('role','Patient')
        users = load_users()
        if email in users:
            error = 'An account with this email already exists.'
        elif len(pw) < 6:
            error = 'Password must be at least 6 characters.'
        else:
            users[email] = {'name':name,'email':email,'role':role,'password':hash_password(pw)}
            save_users(users)
            session['user'] = {'email':email,'name':name,'role':role}
            return redirect(url_for('dashboard'))
    return render_template('signup.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('landing'))

@app.route('/dashboard')
@login_required
def dashboard():
    u = current_user()
    kpis = gen_kpis(u['email'])
    history = gen_history(u['email'])
    return render_template('dashboard.html', user=u, kpis=kpis, history=history)

@app.route('/detection')
@login_required
def detection():
    u = current_user()
    limb = request.args.get('limb','right_arm')
    angles = gen_limb_angles(u['email'], limb)
    kpis = gen_kpis(u['email'])
    return render_template('detection.html', user=u, limb=limb, angles=angles, kpis=kpis)

@app.route('/records')
@login_required
def records():
    u = current_user()
    history = gen_history(u['email'])
    return render_template('records.html', user=u, history=history)

@app.route('/emergency')
@login_required
def emergency():
    u = current_user()
    alerts = gen_alerts(u['email'])
    kpis = gen_kpis(u['email'])
    return render_template('emergency.html', user=u, alerts=alerts, kpis=kpis)

@app.route('/trends')
@login_required
def trends():
    u = current_user()
    history = gen_history(u['email'], 60)
    streak = 0
    for r in reversed(history):
        if r['fall_risk'] < 30:
            streak += 1
        else:
            break
    return render_template('trends.html', user=u, history=history, streak=streak)

# ── API ───────────────────────────────────────────────────────────────────────
@app.route('/faq')
@login_required
def faq():
    return render_template('faq.html', user=current_user())

@app.route('/support')
@login_required
def support():
    return render_template('support.html', user=current_user())

@app.route('/resources')
@login_required
def resources():
    return render_template('resources.html', user=current_user())

@app.route('/api/limb_angles')
@login_required
def api_limb_angles():
    u = current_user()
    limb = request.args.get('limb','right_arm')
    return jsonify(angles=gen_limb_angles(u['email']+str(datetime.now().second), limb))

@app.route('/api/live_kpis')
@login_required
def api_live_kpis():
    u = current_user()
    seed(u['email'] + str(datetime.now().minute))
    return jsonify(gen_kpis(u['email'] + str(datetime.now().minute)))

# ══════════════════════════════════════════════════════════════════════════════
# NEW PROFESSIONAL FEATURES
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/threshold_monitor')
@login_required
def threshold_monitor():
    """
    Professional threshold monitoring dashboard
    Real-time limb angle monitoring with intelligent alerts
    """
    try:
        u = current_user()
        
        # Get current angles for all limbs
        angles = {
            'right_arm': gen_limb_angles(u['email'], 'right_arm')[0],
            'left_arm': gen_limb_angles(u['email'], 'left_arm')[0],
            'right_leg': gen_limb_angles(u['email'], 'right_leg')[0],
            'left_leg': gen_limb_angles(u['email'], 'left_leg')[0]
        }
        
        # Get KPIs
        kpis = gen_kpis(u['email'])
        
        # Check all thresholds
        threshold_report = threshold_checker.check_all_limbs(angles)
        
        # Check brain sync
        brain_check = threshold_checker.check_brain_sync(kpis['brain_corr'])
        
        # Calculate comprehensive risk score
        risk_analysis = threshold_checker.calculate_risk_score(
            angles, 
            kpis['brain_corr'], 
            kpis['posture']
        )
        
        # Send alerts if critical
        critical_alerts = [a for a in threshold_report['alerts'] if a['alert_level'] == 'RED']
        if critical_alerts:
            # In production, get guardian emails from user profile
            guardian_emails = ['guardian@example.com']  # Placeholder
            for alert in critical_alerts:
                notification_service.send_threshold_alert(
                    u['email'], 
                    guardian_emails, 
                    alert
                )
        
        logger.info(f"Threshold monitor accessed by {u['email']} - {threshold_report['alert_count']} alerts")
        
        return render_template(
            'threshold_monitor.html',
            user=u,
            angles=angles,
            threshold_report=threshold_report,
            brain_check=brain_check,
            risk_analysis=risk_analysis,
            kpis=kpis
        )
    
    except Exception as e:
        logger.error(f"Error in threshold_monitor: {e}")
        return render_template('error.html', error="Unable to load threshold monitor"), 500

@app.route('/brain_heatmap')
@login_required
def brain_heatmap():
    """
    24-hour brain activity heatmap visualization
    Shows when brain-limb connection is strongest/weakest
    """
    try:
        u = current_user()
        
        # Generate 24-hour heatmap data
        seed(u['email'])
        heatmap_data = []
        
        for hour in range(24):
            # Simulate brain activity patterns (higher during day, lower at night)
            base_activity = 0.80
            
            # Morning wake-up dip (6-8 AM)
            if 6 <= hour <= 8:
                base_activity = 0.65
            # Post-lunch dip (1-3 PM)
            elif 13 <= hour <= 15:
                base_activity = 0.70
            # Evening decline (10 PM - 3 AM)
            elif hour >= 22 or hour <= 3:
                base_activity = 0.60
            # Peak hours (9-11 AM)
            elif 9 <= hour <= 11:
                base_activity = 0.90
            
            # Add some variance
            activity = base_activity + random.uniform(-0.08, 0.08)
            activity = max(0.5, min(1.0, activity))
            
            heatmap_data.append({
                'hour': hour,
                'activity': round(activity, 2),
                'label': f"{hour:02d}:00",
                'status': 'high' if activity >= 0.80 else 'medium' if activity >= 0.70 else 'low'
            })
        
        # Calculate statistics
        activities = [h['activity'] for h in heatmap_data]
        avg_activity = sum(activities) / len(activities)
        peak_hour = max(heatmap_data, key=lambda x: x['activity'])
        low_hour = min(heatmap_data, key=lambda x: x['activity'])
        
        stats = {
            'average': round(avg_activity, 2),
            'peak_hour': peak_hour['hour'],
            'peak_value': peak_hour['activity'],
            'lowest_hour': low_hour['hour'],
            'lowest_value': low_hour['activity'],
            'high_risk_windows': [h for h in heatmap_data if h['activity'] < 0.70]
        }
        
        logger.info(f"Brain heatmap generated for {u['email']}")
        
        return render_template(
            'brain_heatmap.html',
            user=u,
            heatmap_data=heatmap_data,
            stats=stats,
            kpis=gen_kpis(u['email'])
        )
    
    except Exception as e:
        logger.error(f"Error in brain_heatmap: {e}")
        return render_template('error.html', error="Unable to load brain heatmap"), 500

@app.route('/coordination_matrix')
@login_required
def coordination_matrix():
    """
    Limb coordination matrix - shows inter-limb relationships
    Detects asymmetry and coordination issues
    """
    try:
        u = current_user()
        
        # Get angle series for all limbs
        limbs = ['right_arm', 'left_arm', 'right_leg', 'left_leg']
        angle_series = {}
        
        for limb in limbs:
            angle_series[limb] = gen_limb_angles(u['email'], limb)
        
        # Calculate correlation matrix
        def simple_correlation(series1, series2):
            """Calculate correlation coefficient"""
            n = len(series1)
            sum_x = sum(series1)
            sum_y = sum(series2)
            sum_xy = sum(x*y for x, y in zip(series1, series2))
            sum_x2 = sum(x**2 for x in series1)
            sum_y2 = sum(y**2 for y in series2)
            
            numerator = n * sum_xy - sum_x * sum_y
            denominator = math.sqrt((n * sum_x2 - sum_x**2) * (n * sum_y2 - sum_y**2))
            
            return numerator / denominator if denominator != 0 else 0
        
        correlation_matrix = []
        for i, limb1 in enumerate(limbs):
            row = []
            for j, limb2 in enumerate(limbs):
                if i == j:
                    corr = 100  # Perfect self-correlation
                else:
                    corr = simple_correlation(angle_series[limb1], angle_series[limb2]) * 100
                    corr = round(corr, 1)
                row.append(corr)
            correlation_matrix.append(row)
        
        # Calculate symmetry scores
        right_left_arm = correlation_matrix[0][1]  # right_arm vs left_arm
        right_left_leg = correlation_matrix[2][3]  # right_leg vs left_leg
        
        symmetry_analysis = {
            'arm_symmetry': round(right_left_arm, 1),
            'leg_symmetry': round(right_left_leg, 1),
            'overall_symmetry': round((right_left_arm + right_left_leg) / 2, 1),
            'status': 'Excellent' if right_left_arm > 85 and right_left_leg > 85 else
                     'Good' if right_left_arm > 75 and right_left_leg > 75 else
                     'Fair' if right_left_arm > 65 and right_left_leg > 65 else
                     'Poor'
        }
        
        # Identify abnormal patterns
        abnormal_patterns = []
        if right_left_arm < 70:
            abnormal_patterns.append({
                'type': 'Arm Asymmetry',
                'description': f'Left-right arm correlation is low ({right_left_arm}%). This may indicate unilateral weakness.',
                'severity': 'warning'
            })
        if right_left_leg < 70:
            abnormal_patterns.append({
                'type': 'Leg Asymmetry',
                'description': f'Left-right leg correlation is low ({right_left_leg}%). Gait analysis recommended.',
                'severity': 'warning'
            })
        
        logger.info(f"Coordination matrix generated for {u['email']}")
        
        return render_template(
            'coordination_matrix.html',
            user=u,
            limbs=limbs,
            correlation_matrix=correlation_matrix,
            symmetry_analysis=symmetry_analysis,
            abnormal_patterns=abnormal_patterns,
            angle_series=angle_series,
            kpis=gen_kpis(u['email'])
        )
    
    except Exception as e:
        logger.error(f"Error in coordination_matrix: {e}")
        return render_template('error.html', error="Unable to load coordination matrix"), 500

@app.route('/neural_fatigue')
@login_required
def neural_fatigue():
    """
    Neural fatigue monitoring
    Tracks cognitive-motor fatigue throughout the day
    """
    try:
        u = current_user()
        kpis = gen_kpis(u['email'])
        
        # Calculate fatigue score
        seed(u['email'])
        
        # Simulate hourly fatigue data for today
        current_hour = datetime.now().hour
        hourly_fatigue = []
        
        for hour in range(24):
            # Base fatigue increases throughout the day
            base_fatigue = (hour / 24) * 50
            
            # Add peaks during known high-fatigue periods
            if 6 <= hour <= 8:  # Morning wake-up
                base_fatigue += 15
            elif 13 <= hour <= 15:  # Post-lunch
                base_fatigue += 10
            elif hour >= 22 or hour <= 3:  # Night/early morning
                base_fatigue += 25
            
            # Add variance
            fatigue = base_fatigue + random.uniform(-5, 5)
            fatigue = max(0, min(100, fatigue))
            
            hourly_fatigue.append({
                'hour': hour,
                'fatigue': round(fatigue, 1),
                'status': 'high' if fatigue >= 60 else 'moderate' if fatigue >= 30 else 'low',
                'label': f"{hour:02d}:00"
            })
        
        # Current fatigue
        current_fatigue = hourly_fatigue[current_hour]
        
        # Calculate statistics
        avg_fatigue = sum(h['fatigue'] for h in hourly_fatigue) / len(hourly_fatigue)
        peak_fatigue = max(hourly_fatigue, key=lambda x: x['fatigue'])
        
        # Generate recommendations
        recommendations = []
        if current_fatigue['fatigue'] >= 60:
            recommendations = [
                "🛑 High fatigue detected - Immediate rest recommended",
                "💤 Avoid physical activities for next 2 hours",
                "💧 Stay hydrated - fatigue increases dehydration risk",
                "📞 Notify caregiver of your fatigue level"
            ]
        elif current_fatigue['fatigue'] >= 30:
            recommendations = [
                "⚠️ Moderate fatigue - Take a 15-minute rest break",
                "🪑 Sit down for activities requiring concentration",
                "🚶 Reduce walking speed and movement intensity"
            ]
        else:
            recommendations = [
                "✓ Low fatigue - Continue normal activities",
                "💪 Good time for physical therapy exercises",
                "📊 Maintain current activity level"
            ]
        
        logger.info(f"Neural fatigue data generated for {u['email']}")
        
        return render_template(
            'neural_fatigue.html',
            user=u,
            current_fatigue=current_fatigue,
            hourly_fatigue=hourly_fatigue,
            avg_fatigue=round(avg_fatigue, 1),
            peak_fatigue=peak_fatigue,
            recommendations=recommendations,
            kpis=kpis
        )
    
    except Exception as e:
        logger.error(f"Error in neural_fatigue: {e}")
        return render_template('error.html', error="Unable to load neural fatigue monitor"), 500

# ── Enhanced API Routes ───────────────────────────────────────────────────────

@app.route('/api/threshold_check')
@login_required
def api_threshold_check():
    """
    API endpoint for real-time threshold checking
    Returns current status of all limbs and alerts
    """
    try:
        u = current_user()
        
        # Get current angles
        angles = {
            'right_arm': gen_limb_angles(u['email'] + str(datetime.now().second), 'right_arm')[0],
            'left_arm': gen_limb_angles(u['email'] + str(datetime.now().second), 'left_arm')[0],
            'right_leg': gen_limb_angles(u['email'] + str(datetime.now().second), 'right_leg')[0],
            'left_leg': gen_limb_angles(u['email'] + str(datetime.now().second), 'left_leg')[0]
        }
        
        # Check thresholds
        report = threshold_checker.check_all_limbs(angles)
        
        return jsonify({
            'success': True,
            'angles': angles,
            'report': report,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"API threshold_check error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/send_sos', methods=['POST'])
@login_required
def api_send_sos():
    """
    Send emergency SOS alert
    """
    try:
        u = current_user()
        
        # Get location and vitals from request (if provided)
        data = request.get_json() or {}
        location = data.get('location')
        vitals = data.get('vitals', {})
        
        # In production, get guardian emails from user profile
        guardian_emails = ['guardian@example.com']  # Placeholder
        
        # Send SOS
        result = notification_service.send_sos_alert(
            u['email'],
            guardian_emails,
            location,
            vitals
        )
        
        logger.critical(f"SOS activated by {u['email']}")
        
        return jsonify({
            'success': True,
            'message': 'SOS alert sent successfully',
            'notification': result
        })
    
    except Exception as e:
        logger.error(f"API send_sos error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/acknowledge_alert/<notification_id>', methods=['POST'])
@login_required
def api_acknowledge_alert(notification_id):
    """
    Acknowledge an alert
    """
    try:
        u = current_user()
        success = notification_service.acknowledge_alert(notification_id, u['email'])
        
        return jsonify({
            'success': success,
            'message': 'Alert acknowledged' if success else 'Alert not found'
        })
    
    except Exception as e:
        logger.error(f"API acknowledge_alert error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ── Error Handlers ────────────────────────────────────────────────────────────

@app.errorhandler(404)
def not_found(error):
    return render_template('error.html', error="Page not found"), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal error: {error}")
    return render_template('error.html', error="Internal server error"), 500

if __name__ == '__main__':
    logger.info("Starting FallVision server on port 5000...")
    app.run(debug=True, port=5000)