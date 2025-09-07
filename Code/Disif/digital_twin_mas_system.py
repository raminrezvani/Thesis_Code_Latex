#!/usr/bin/env python3
"""
Advanced Multi-Agent System for Smart Traffic Digital Twin
Specialized agents for traffic, weather, air quality, and infrastructure monitoring
Intelligent decision making and real-time traffic management
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import logging
import threading
import queue
import json
from pathlib import Path
from datetime import datetime, timedelta
import random

# Import existing MAS components
# Note: We're using our own coordinator and base agent classes for compatibility

# Create a compatible coordinator class for digital twin system
class DigitalTwinCoordinator:
    """Coordinator class compatible with digital twin system"""
    
    def __init__(self):
        self.worker_agents = []
        self.master_agent = None
        self.is_running = False
        
    def receive_message(self, message):
        """Receive message from agents"""
        print(f"Coordinator received message: {message.get('type', 'unknown')}")
        
    def start_system(self):
        """Start the system"""
        self.is_running = True
        return True
        
    def stop_system(self):
        """Stop the system"""
        self.is_running = False
        
    def get_system_status(self):
        """Get system status"""
        return {"status": "running" if self.is_running else "stopped"}

# Create a compatible base agent class for digital twin system
class BaseAgent:
    """Base class for all digital twin agents"""
    
    def __init__(self, agent_id, coordinator):
        self.agent_id = agent_id
        self.coordinator = coordinator
        self.agent_type = "base_agent"
        self.is_running = False
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        """Setup logging for the agent"""
        import logging
        logger = logging.getLogger(f"{self.agent_type}_{self.agent_id}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def log_info(self, message):
        """Log info message"""
        self.logger.info(message)
    
    def log_error(self, message):
        """Log error message"""
        self.logger.error(message)
    
    def send_to_coordinator(self, message):
        """Send message to coordinator"""
        if hasattr(self.coordinator, 'receive_message'):
            self.coordinator.receive_message(message)
        else:
            self.log_error(f"Coordinator has no receive_message method")
    
    def set_message_handler(self, handler):
        """Set message handler for the agent"""
        self.message_handler = handler
    
    def receive_message(self, message):
        """Receive message from coordinator"""
        if hasattr(self, 'message_handler') and self.message_handler:
            self.message_handler(message)
        else:
            self.log_info(f"Received message: {message.get('type', 'unknown')}")

class DigitalTwinTrafficAgent(BaseAgent):
    """Specialized agent for traffic monitoring and analysis"""
    
    def __init__(self, agent_id, coordinator):
        super().__init__(agent_id, coordinator)
        self.agent_type = "traffic_monitor"
        self.traffic_data = []
        self.congestion_alerts = []
        self.traffic_patterns = {}
        self.vehicle_behavior_analysis = {}
        
    def process_traffic_data(self, data):
        """Process incoming traffic data and detect patterns"""
        self.traffic_data.append(data)
        
        # Analyze vehicle behavior
        self._analyze_vehicle_behavior(data)
        
        # Detect congestion patterns
        if self._detect_congestion(data):
            self._create_congestion_alert(data)
        
        # Update traffic patterns
        self._update_traffic_patterns(data)
        
        # Send processed data to coordinator
        self.send_to_coordinator({
            "type": "traffic_analysis",
            "agent_id": self.agent_id,
            "timestamp": datetime.now().isoformat(),
            "data": data,
            "analysis": {
                "congestion_detected": len(self.congestion_alerts) > 0,
                "pattern_updated": True,
                "behavior_analyzed": True
            }
        })
    
    def _analyze_vehicle_behavior(self, data):
        """Analyze individual vehicle behavior patterns"""
        vehicle_id = data.get('vehicle_id')
        if vehicle_id not in self.vehicle_behavior_analysis:
            self.vehicle_behavior_analysis[vehicle_id] = {
                'speed_history': [],
                'acceleration_patterns': [],
                'braking_patterns': [],
                'risk_score': 0
            }
        
        behavior = self.vehicle_behavior_analysis[vehicle_id]
        behavior['speed_history'].append(data.get('speed', 0))
        behavior['acceleration_patterns'].append(data.get('acceleration', 0))
        behavior['braking_patterns'].append(data.get('braking_frequency', 0))
        
        # Calculate risk score based on behavior
        risk_score = self._calculate_risk_score(behavior)
        behavior['risk_score'] = risk_score
        
        # Keep only last 100 records for performance
        if len(behavior['speed_history']) > 100:
            behavior['speed_history'] = behavior['speed_history'][-100:]
            behavior['acceleration_patterns'] = behavior['acceleration_patterns'][-100:]
            behavior['braking_patterns'] = behavior['braking_patterns'][-100:]
    
    def _calculate_risk_score(self, behavior):
        """Calculate risk score for vehicle behavior"""
        if not behavior['speed_history']:
            return 0
        
        avg_speed = sum(behavior['speed_history']) / len(behavior['speed_history'])
        max_acceleration = max(behavior['acceleration_patterns']) if behavior['acceleration_patterns'] else 0
        avg_braking = sum(behavior['braking_patterns']) / len(behavior['braking_patterns']) if behavior['braking_patterns'] else 0
        
        risk_score = 0
        
        # Speed risk (0-40 points)
        if avg_speed > 80:
            risk_score += 40
        elif avg_speed > 60:
            risk_score += 20
        elif avg_speed > 40:
            risk_score += 10
        
        # Acceleration risk (0-30 points)
        if max_acceleration > 6:
            risk_score += 30
        elif max_acceleration > 4:
            risk_score += 20
        elif max_acceleration > 2:
            risk_score += 10
        
        # Braking risk (0-30 points)
        if avg_braking > 8:
            risk_score += 30
        elif avg_braking > 5:
            risk_score += 20
        elif avg_braking > 2:
            risk_score += 10
        
        return min(100, risk_score)
    
    def _detect_congestion(self, data):
        """Detect traffic congestion based on speed and vehicle density"""
        speed = data.get('speed', 0)
        location_id = data.get('location_id')
        
        # Simple congestion detection: speed < 20 km/h
        if speed < 20:
            # Check if this location already has congestion
            existing_congestion = [c for c in self.congestion_alerts if c['location_id'] == location_id]
            if not existing_congestion:
                return True
        
        return False
    
    def _create_congestion_alert(self, data):
        """Create congestion alert for traffic management"""
        alert = {
            'alert_id': f"CONG_{len(self.congestion_alerts) + 1}",
            'timestamp': datetime.now().isoformat(),
            'location_id': data.get('location_id'),
            'severity': 'high' if data.get('speed', 0) < 10 else 'medium',
            'vehicle_count': len([v for v in self.traffic_data if v.get('location_id') == data.get('location_id')]),
            'avg_speed': data.get('speed', 0),
            'recommendation': self._generate_congestion_recommendation(data)
        }
        
        self.congestion_alerts.append(alert)
        
        # Send alert to coordinator
        self.send_to_coordinator({
            "type": "congestion_alert",
            "agent_id": self.agent_id,
            "alert": alert
        })
    
    def _generate_congestion_recommendation(self, data):
        """Generate recommendations for congestion management"""
        speed = data.get('speed', 0)
        vehicle_type = data.get('vehicle_type', 'car')
        
        if speed < 10:
            return "Critical congestion - activate emergency traffic management protocols"
        elif speed < 20:
            return "Moderate congestion - adjust traffic light timing and suggest alternative routes"
        else:
            return "Minor congestion - monitor situation and prepare for traffic light optimization"
    
    def _update_traffic_patterns(self, data):
        """Update traffic patterns for predictive analysis"""
        hour = datetime.now().hour
        location_id = data.get('location_id')
        
        if location_id not in self.traffic_patterns:
            self.traffic_patterns[location_id] = {}
        
        if hour not in self.traffic_patterns[location_id]:
            self.traffic_patterns[location_id][hour] = {
                'vehicle_count': 0,
                'avg_speed': 0,
                'congestion_events': 0
            }
        
        pattern = self.traffic_patterns[location_id][hour]
        pattern['vehicle_count'] += 1
        pattern['avg_speed'] = (pattern['avg_speed'] * (pattern['vehicle_count'] - 1) + data.get('speed', 0)) / pattern['vehicle_count']
        
        if data.get('speed', 0) < 20:
            pattern['congestion_events'] += 1

class DigitalTwinWeatherAgent(BaseAgent):
    """Specialized agent for weather monitoring and traffic impact analysis"""
    
    def __init__(self, agent_id, coordinator):
        super().__init__(agent_id, coordinator)
        self.agent_type = "weather_monitor"
        self.weather_data = []
        self.weather_alerts = []
        self.traffic_impact_assessment = {}
        
    def process_weather_data(self, data):
        """Process weather data and assess traffic impact"""
        self.weather_data.append(data)
        
        # Assess traffic impact
        impact = self._assess_traffic_impact(data)
        
        # Create weather alerts if needed
        if self._should_create_weather_alert(data):
            self._create_weather_alert(data, impact)
        
        # Update traffic impact assessment
        self._update_traffic_impact(data, impact)
        
        # Send processed data to coordinator
        self.send_to_coordinator({
            "type": "weather_analysis",
            "agent_id": self.agent_id,
            "timestamp": datetime.now().isoformat(),
            "data": data,
            "traffic_impact": impact
        })
    
    def _assess_traffic_impact(self, data):
        """Assess how weather conditions affect traffic"""
        condition = data.get('condition', 'sunny')
        visibility = data.get('visibility', 1.0)
        precipitation = data.get('precipitation', 0)
        wind_speed = data.get('wind_speed', 0)
        
        impact_score = 0
        impact_factors = []
        
        # Visibility impact
        if visibility < 0.3:
            impact_score += 40
            impact_factors.append("Severe visibility reduction")
        elif visibility < 0.6:
            impact_score += 25
            impact_factors.append("Moderate visibility reduction")
        elif visibility < 0.8:
            impact_score += 15
            impact_factors.append("Slight visibility reduction")
        
        # Precipitation impact
        if precipitation > 5:
            impact_score += 30
            impact_factors.append("Heavy precipitation")
        elif precipitation > 2:
            impact_score += 20
            impact_factors.append("Moderate precipitation")
        elif precipitation > 0:
            impact_score += 10
            impact_factors.append("Light precipitation")
        
        # Wind impact
        if wind_speed > 20:
            impact_score += 25
            impact_factors.append("High winds")
        elif wind_speed > 15:
            impact_score += 15
            impact_factors.append("Moderate winds")
        
        # Weather condition specific impacts
        if condition == 'foggy':
            impact_score += 35
            impact_factors.append("Fog conditions")
        elif condition == 'snowy':
            impact_score += 45
            impact_factors.append("Snow conditions")
        elif condition == 'rainy':
            impact_score += 25
            impact_factors.append("Rain conditions")
        
        return {
            'score': min(100, impact_score),
            'level': 'high' if impact_score > 60 else 'medium' if impact_score > 30 else 'low',
            'factors': impact_factors,
            'recommendations': self._generate_weather_recommendations(impact_score, condition)
        }
    
    def _should_create_weather_alert(self, data):
        """Determine if weather alert should be created"""
        impact = self._assess_traffic_impact(data)
        return impact['score'] > 50  # Alert for medium-high impact weather
    
    def _create_weather_alert(self, data, impact):
        """Create weather alert for traffic management"""
        alert = {
            'alert_id': f"WEATHER_{len(self.weather_alerts) + 1}",
            'timestamp': datetime.now().isoformat(),
            'location_id': data.get('location_id'),
            'weather_condition': data.get('condition'),
            'impact_score': impact['score'],
            'impact_level': impact['level'],
            'impact_factors': impact['factors'],
            'recommendations': impact['recommendations']
        }
        
        self.weather_alerts.append(alert)
        
        # Send alert to coordinator
        self.send_to_coordinator({
            "type": "weather_alert",
            "agent_id": self.agent_id,
            "alert": alert
        })
    
    def _generate_weather_recommendations(self, impact_score, condition):
        """Generate traffic management recommendations for weather conditions"""
        recommendations = []
        
        if impact_score > 70:
            recommendations.append("Activate emergency traffic protocols")
            recommendations.append("Reduce speed limits by 30-50%")
            recommendations.append("Increase traffic light intervals")
        elif impact_score > 50:
            recommendations.append("Reduce speed limits by 20-30%")
            recommendations.append("Increase traffic light intervals")
            recommendations.append("Activate variable message signs")
        elif impact_score > 30:
            recommendations.append("Monitor traffic conditions closely")
            recommendations.append("Prepare for speed limit adjustments")
        
        # Condition-specific recommendations
        if condition == 'foggy':
            recommendations.append("Increase lighting at intersections")
            recommendations.append("Activate fog warning systems")
        elif condition == 'snowy':
            recommendations.append("Activate snow removal protocols")
            recommendations.append("Close hazardous road sections if necessary")
        elif condition == 'rainy':
            recommendations.append("Monitor drainage systems")
            recommendations.append("Adjust traffic light timing for wet conditions")
        
        return recommendations
    
    def _update_traffic_impact(self, data, impact):
        """Update traffic impact assessment for historical analysis"""
        location_id = data.get('location_id')
        hour = datetime.now().hour
        
        if location_id not in self.traffic_impact_assessment:
            self.traffic_impact_assessment[location_id] = {}
        
        if hour not in self.traffic_impact_assessment[location_id]:
            self.traffic_impact_assessment[location_id][hour] = {
                'total_impact_score': 0,
                'weather_events': 0,
                'avg_impact': 0
            }
        
        assessment = self.traffic_impact_assessment[location_id][hour]
        assessment['total_impact_score'] += impact['score']
        assessment['weather_events'] += 1
        assessment['avg_impact'] = assessment['total_impact_score'] / assessment['weather_events']

class DigitalTwinAirQualityAgent(BaseAgent):
    """Specialized agent for air quality monitoring and traffic correlation"""
    
    def __init__(self, agent_id, coordinator):
        super().__init__(agent_id, coordinator)
        self.agent_type = "air_quality_monitor"
        self.air_quality_data = []
        self.pollution_alerts = []
        self.traffic_correlation = {}
        
    def process_air_quality_data(self, data):
        """Process air quality data and correlate with traffic"""
        self.air_quality_data.append(data)
        
        # Check for pollution alerts
        if self._should_create_pollution_alert(data):
            self._create_pollution_alert(data)
        
        # Correlate with traffic patterns
        self._correlate_with_traffic(data)
        
        # Send processed data to coordinator
        self.send_to_coordinator({
            "type": "air_quality_analysis",
            "agent_id": self.agent_id,
            "timestamp": datetime.now().isoformat(),
            "data": data,
            "pollution_level": data.get('quality_level'),
            'correlation_analysis': self._get_correlation_summary()
        })
    
    def _should_create_pollution_alert(self, data):
        """Determine if pollution alert should be created"""
        quality_level = data.get('quality_level', 'good')
        pm25 = data.get('pm25', 0)
        co = data.get('co', 0)
        
        return quality_level in ['poor', 'very_poor'] or pm25 > 55 or co > 6
    
    def _create_pollution_alert(self, data):
        """Create pollution alert for traffic management"""
        alert = {
            'alert_id': f"POLLUTION_{len(self.pollution_alerts) + 1}",
            'timestamp': datetime.now().isoformat(),
            'location_id': data.get('location_id'),
            'quality_level': data.get('quality_level'),
            'pm25': data.get('pm25'),
            'co': data.get('co'),
            'no2': data.get('no2'),
            'o3': data.get('o3'),
            'recommendations': self._generate_pollution_recommendations(data)
        }
        
        self.pollution_alerts.append(alert)
        
        # Send alert to coordinator
        self.send_to_coordinator({
            "type": "pollution_alert",
            "agent_id": self.agent_id,
            "alert": alert
        })
    
    def _generate_pollution_recommendations(self, data):
        """Generate traffic management recommendations for pollution control"""
        quality_level = data.get('quality_level', 'good')
        recommendations = []
        
        if quality_level == 'very_poor':
            recommendations.append("Implement emergency traffic restrictions")
            recommendations.append("Close high-pollution areas to heavy vehicles")
            recommendations.append("Activate public transport priority systems")
        elif quality_level == 'poor':
            recommendations.append("Implement traffic flow optimization")
            recommendations.append("Encourage public transport usage")
            recommendations.append("Activate pollution-based traffic routing")
        elif quality_level == 'moderate':
            recommendations.append("Monitor air quality trends")
            recommendations.append("Prepare for traffic management if conditions worsen")
        
        return recommendations
    
    def _correlate_with_traffic(self, data):
        """Correlate air quality with traffic patterns"""
        location_id = data.get('location_id')
        hour = datetime.now().hour
        
        if location_id not in self.traffic_correlation:
            self.traffic_correlation[location_id] = {}
        
        if hour not in self.traffic_correlation[location_id]:
            self.traffic_correlation[location_id][hour] = {
                'air_quality_samples': [],
                'traffic_intensity': 0,
                'correlation_coefficient': 0
            }
        
        correlation = self.traffic_correlation[location_id][hour]
        correlation['air_quality_samples'].append({
            'pm25': data.get('pm25', 0),
            'co': data.get('co', 0),
            'timestamp': datetime.now()
        })
        
        # Keep only last 24 samples
        if len(correlation['air_quality_samples']) > 24:
            correlation['air_quality_samples'] = correlation['air_quality_samples'][-24:]
    
    def _get_correlation_summary(self):
        """Get summary of air quality and traffic correlation"""
        summary = {}
        for location_id, hours in self.traffic_correlation.items():
            summary[location_id] = {}
            for hour, data in hours.items():
                if data['air_quality_samples']:
                    avg_pm25 = sum(s['pm25'] for s in data['air_quality_samples']) / len(data['air_quality_samples'])
                    avg_co = sum(s['co'] for s in data['air_quality_samples']) / len(data['air_quality_samples'])
                    summary[location_id][hour] = {
                        'avg_pm25': round(avg_pm25, 2),
                        'avg_co': round(avg_co, 2),
                        'samples_count': len(data['air_quality_samples'])
                    }
        return summary

class DigitalTwinInfrastructureAgent(BaseAgent):
    """Specialized agent for infrastructure monitoring and maintenance alerts"""
    
    def __init__(self, agent_id, coordinator):
        super().__init__(agent_id, coordinator)
        self.agent_type = "infrastructure_monitor"
        self.infrastructure_data = []
        self.maintenance_alerts = []
        self.health_trends = {}
        
    def process_infrastructure_data(self, data):
        """Process infrastructure data and detect maintenance needs"""
        self.infrastructure_data.append(data)
        
        # Check for maintenance alerts
        if self._should_create_maintenance_alert(data):
            self._create_maintenance_alert(data)
        
        # Update health trends
        self._update_health_trends(data)
        
        # Send processed data to coordinator
        self.send_to_coordinator({
            "type": "infrastructure_analysis",
            "agent_id": self.agent_id,
            "timestamp": datetime.now().isoformat(),
            "data": data,
            "health_status": self._assess_health_status(data),
            'maintenance_needed': len(self.maintenance_alerts) > 0
        })
    
    def _should_create_maintenance_alert(self, data):
        """Determine if maintenance alert should be created"""
        structural_health = data.get('structural_health', 1.0)
        traffic_light_status = data.get('traffic_light_status', 'operational')
        
        return structural_health < 0.7 or traffic_light_status != 'operational'
    
    def _create_maintenance_alert(self, data):
        """Create maintenance alert for infrastructure management"""
        alert = {
            'alert_id': f"MAINTENANCE_{len(self.maintenance_alerts) + 1}",
            'timestamp': datetime.now().isoformat(),
            'location_id': data.get('location_id'),
            'location_type': data.get('location_type'),
            'structural_health': data.get('structural_health'),
            'traffic_light_status': data.get('traffic_light_status'),
            'priority': self._calculate_maintenance_priority(data),
            'recommendations': self._generate_maintenance_recommendations(data)
        }
        
        self.maintenance_alerts.append(alert)
        
        # Send alert to coordinator
        self.send_to_coordinator({
            "type": "maintenance_alert",
            "agent_id": self.agent_id,
            "alert": alert
        })
    
    def _calculate_maintenance_priority(self, data):
        """Calculate maintenance priority based on infrastructure health"""
        structural_health = data.get('structural_health', 1.0)
        traffic_light_status = data.get('traffic_light_status', 'operational')
        
        if structural_health < 0.5 or traffic_light_status == 'malfunction':
            return 'critical'
        elif structural_health < 0.7 or traffic_light_status == 'maintenance_needed':
            return 'high'
        elif structural_health < 0.8:
            return 'medium'
        else:
            return 'low'
    
    def _generate_maintenance_recommendations(self, data):
        """Generate maintenance recommendations"""
        recommendations = []
        structural_health = data.get('structural_health', 1.0)
        traffic_light_status = data.get('traffic_light_status', 'operational')
        
        if structural_health < 0.5:
            recommendations.append("Immediate structural inspection required")
            recommendations.append("Consider temporary closure if safety is compromised")
        elif structural_health < 0.7:
            recommendations.append("Schedule structural maintenance within 48 hours")
            recommendations.append("Implement traffic restrictions if necessary")
        
        if traffic_light_status == 'malfunction':
            recommendations.append("Immediate traffic light repair required")
            recommendations.append("Implement manual traffic control")
        elif traffic_light_status == 'maintenance_needed':
            recommendations.append("Schedule traffic light maintenance within 24 hours")
        
        return recommendations
    
    def _assess_health_status(self, data):
        """Assess overall infrastructure health status"""
        structural_health = data.get('structural_health', 1.0)
        
        if structural_health >= 0.9:
            return 'excellent'
        elif structural_health >= 0.8:
            return 'good'
        elif structural_health >= 0.7:
            return 'fair'
        elif structural_health >= 0.5:
            return 'poor'
        else:
            return 'critical'
    
    def _update_health_trends(self, data):
        """Update infrastructure health trends for predictive maintenance"""
        location_id = data.get('location_id')
        timestamp = datetime.now()
        
        if location_id not in self.health_trends:
            self.health_trends[location_id] = []
        
        self.health_trends[location_id].append({
            'timestamp': timestamp,
            'structural_health': data.get('structural_health'),
            'traffic_light_status': data.get('traffic_light_status')
        })
        
        # Keep only last 168 records (1 week of hourly data)
        if len(self.health_trends[location_id]) > 168:
            self.health_trends[location_id] = self.health_trends[location_id][-168:]

class DigitalTwinMasterAgent(BaseAgent):
    """Master agent for coordinating all digital twin agents and making decisions"""
    
    def __init__(self, agent_id, coordinator):
        super().__init__(agent_id, coordinator)
        self.agent_type = "digital_twin_master"
        self.traffic_agents = []
        self.weather_agents = []
        self.air_quality_agents = []
        self.infrastructure_agents = []
        self.integrated_alerts = []
        self.decision_history = []
        
    def register_agent(self, agent):
        """Register specialized agents with the master agent"""
        if agent.agent_type == "traffic_monitor":
            self.traffic_agents.append(agent)
        elif agent.agent_type == "weather_monitor":
            self.weather_agents.append(agent)
        elif agent.agent_type == "air_quality_monitor":
            self.air_quality_agents.append(agent)
        elif agent.agent_type == "infrastructure_monitor":
            self.infrastructure_agents.append(agent)
        
        self.log_info(f"Registered {agent.agent_type} agent: {agent.agent_id}")
    
    def process_integrated_data(self, data):
        """Process integrated data from all specialized agents"""
        # Integrate data from different sources
        integrated_analysis = self._integrate_data_sources(data)
        
        # Make intelligent decisions
        decisions = self._make_intelligent_decisions(integrated_analysis)
        
        # Execute decisions
        self._execute_decisions(decisions)
        
        # Log decision history
        self.decision_history.append({
            'timestamp': datetime.now().isoformat(),
            'integrated_analysis': integrated_analysis,
            'decisions': decisions,
            'execution_status': 'completed'
        })
        
        # Send integrated analysis to coordinator
        self.send_to_coordinator({
            "type": "integrated_analysis",
            "agent_id": self.agent_id,
            "timestamp": datetime.now().isoformat(),
            "analysis": integrated_analysis,
            "decisions": decisions
        })
    
    def _integrate_data_sources(self, data):
        """Integrate data from multiple sources for comprehensive analysis"""
        integration = {
            'traffic_status': self._get_traffic_status(),
            'weather_impact': self._get_weather_impact(),
            'air_quality_status': self._get_air_quality_status(),
            'infrastructure_status': self._get_infrastructure_status(),
            'cross_domain_correlations': self._analyze_cross_domain_correlations()
        }
        
        return integration
    
    def _get_traffic_status(self):
        """Get comprehensive traffic status from all traffic agents"""
        status = {
            'total_vehicles': 0,
            'congestion_alerts': [],
            'high_risk_vehicles': [],
            'traffic_patterns': {}
        }
        
        for agent in self.traffic_agents:
            status['congestion_alerts'].extend(agent.congestion_alerts)
            status['high_risk_vehicles'].extend([
                v for v in agent.vehicle_behavior_analysis.values() 
                if v['risk_score'] > 70
            ])
            status['traffic_patterns'].update(agent.traffic_patterns)
        
        return status
    
    def _get_weather_impact(self):
        """Get weather impact assessment from all weather agents"""
        impact = {
            'active_alerts': [],
            'traffic_impact_scores': {},
            'recommendations': []
        }
        
        for agent in self.weather_agents:
            impact['active_alerts'].extend(agent.weather_alerts)
            impact['traffic_impact_scores'].update(agent.traffic_impact_assessment)
        
        return impact
    
    def _get_air_quality_status(self):
        """Get air quality status from all air quality agents"""
        status = {
            'active_alerts': [],
            'pollution_levels': {},
            'traffic_correlations': {}
        }
        
        for agent in self.air_quality_agents:
            status['active_alerts'].extend(agent.pollution_alerts)
            status['traffic_correlations'].update(agent.traffic_correlation)
        
        return status
    
    def _get_infrastructure_status(self):
        """Get infrastructure status from all infrastructure agents"""
        status = {
            'active_alerts': [],
            'health_trends': {},
            'maintenance_priorities': {}
        }
        
        for agent in self.infrastructure_agents:
            status['active_alerts'].extend(agent.maintenance_alerts)
            status['health_trends'].update(agent.health_trends)
        
        return status
    
    def _analyze_cross_domain_correlations(self):
        """Analyze correlations between different data domains"""
        correlations = {
            'weather_traffic_correlation': self._correlate_weather_traffic(),
            'pollution_traffic_correlation': self._correlate_pollution_traffic(),
            'infrastructure_traffic_correlation': self._correlate_infrastructure_traffic()
        }
        
        return correlations
    
    def _correlate_weather_traffic(self):
        """Correlate weather conditions with traffic patterns"""
        # Implementation would analyze weather impact on traffic
        return {"correlation_strength": "moderate", "impact_factor": 0.6}
    
    def _correlate_pollution_traffic(self):
        """Correlate air quality with traffic patterns"""
        # Implementation would analyze pollution correlation with traffic
        return {"correlation_strength": "strong", "impact_factor": 0.8}
    
    def _correlate_infrastructure_traffic(self):
        """Correlate infrastructure health with traffic patterns"""
        # Implementation would analyze infrastructure impact on traffic
        return {"correlation_strength": "strong", "impact_factor": 0.7}
    
    def _make_intelligent_decisions(self, integrated_analysis):
        """Make intelligent decisions based on integrated analysis"""
        decisions = []
        
        # Traffic management decisions
        if integrated_analysis['traffic_status']['congestion_alerts']:
            decisions.append(self._create_traffic_management_decision(integrated_analysis))
        
        # Weather-based decisions
        if integrated_analysis['weather_impact']['active_alerts']:
            decisions.append(self._create_weather_management_decision(integrated_analysis))
        
        # Pollution control decisions
        if integrated_analysis['air_quality_status']['active_alerts']:
            decisions.append(self._create_pollution_control_decision(integrated_analysis))
        
        # Infrastructure maintenance decisions
        if integrated_analysis['infrastructure_status']['active_alerts']:
            decisions.append(self._create_maintenance_decision(integrated_analysis))
        
        return decisions
    
    def _create_traffic_management_decision(self, analysis):
        """Create traffic management decision based on analysis"""
        return {
            'type': 'traffic_management',
            'priority': 'high',
            'actions': [
                'optimize_traffic_light_timing',
                'activate_dynamic_speed_limits',
                'implement_traffic_rerouting'
            ],
            'reasoning': 'Congestion detected in multiple locations'
        }
    
    def _create_weather_management_decision(self, analysis):
        """Create weather management decision based on analysis"""
        return {
            'type': 'weather_management',
            'priority': 'medium',
            'actions': [
                'adjust_traffic_light_intervals',
                'activate_weather_warning_systems',
                'implement_speed_reductions'
            ],
            'reasoning': 'Weather conditions affecting traffic flow'
        }
    
    def _create_pollution_control_decision(self, analysis):
        """Create pollution control decision based on analysis"""
        return {
            'type': 'pollution_control',
            'priority': 'medium',
            'actions': [
                'implement_traffic_restrictions',
                'activate_public_transport_priority',
                'optimize_traffic_flow_for_emissions'
            ],
            'reasoning': 'Air quality below acceptable levels'
        }
    
    def _create_maintenance_decision(self, analysis):
        """Create maintenance decision based on analysis"""
        return {
            'type': 'infrastructure_maintenance',
            'priority': 'high',
            'actions': [
                'schedule_immediate_maintenance',
                'implement_traffic_restrictions',
                'activate_alternative_routes'
            ],
            'reasoning': 'Infrastructure requires immediate attention'
        }
    
    def _execute_decisions(self, decisions):
        """Execute the decided actions"""
        for decision in decisions:
            self.log_info(f"Executing decision: {decision['type']}")
            
            # Send decision to coordinator for execution
            self.send_to_coordinator({
                "type": "decision_execution",
                "agent_id": self.agent_id,
                "decision": decision,
                "timestamp": datetime.now().isoformat()
            })

class DigitalTwinMASCoordinator(DigitalTwinCoordinator):
    """Coordinator for the Digital Twin MAS system"""
    
    def __init__(self):
        super().__init__()
        self.digital_twin_master = None
        self.traffic_agents = []
        self.weather_agents = []
        self.air_quality_agents = []
        self.infrastructure_agents = []
        
    def initialize_digital_twin_system(self):
        """Initialize the complete Digital Twin MAS system"""
        print("🚀 Initializing Digital Twin MAS System...")
        
        # Create specialized agents
        self._create_specialized_agents()
        
        # Create master agent
        self._create_master_agent()
        
        # Register agents with master
        self._register_agents_with_master()
        
        # Initialize communication channels
        self._initialize_communication()
        
        print("✅ Digital Twin MAS System initialized successfully!")
        return True
    
    def _create_specialized_agents(self):
        """Create specialized agents for different domains"""
        # Create traffic monitoring agents
        for i in range(3):  # 3 traffic agents for different zones
            agent = DigitalTwinTrafficAgent(f"traffic_agent_{i+1}", self)
            self.traffic_agents.append(agent)
            self.worker_agents.append(agent)
        
        # Create weather monitoring agents
        for i in range(2):  # 2 weather agents for different regions
            agent = DigitalTwinWeatherAgent(f"weather_agent_{i+1}", self)
            self.weather_agents.append(agent)
            self.worker_agents.append(agent)
        
        # Create air quality monitoring agents
        for i in range(2):  # 2 air quality agents
            agent = DigitalTwinAirQualityAgent(f"air_quality_agent_{i+1}", self)
            self.air_quality_agents.append(agent)
            self.worker_agents.append(agent)
        
        # Create infrastructure monitoring agents
        for i in range(2):  # 2 infrastructure agents
            agent = DigitalTwinInfrastructureAgent(f"infrastructure_agent_{i+1}", self)
            self.infrastructure_agents.append(agent)
            self.worker_agents.append(agent)
        
        print(f"Created {len(self.worker_agents)} specialized agents")
    
    def _create_master_agent(self):
        """Create the master agent for coordination"""
        self.digital_twin_master = DigitalTwinMasterAgent("digital_twin_master", self)
        self.master_agent = self.digital_twin_master
        print("Created Digital Twin Master Agent")
    
    def _register_agents_with_master(self):
        """Register all specialized agents with the master agent"""
        for agent in self.worker_agents:
            self.digital_twin_master.register_agent(agent)
        
        print("All agents registered with master agent")
    
    def _initialize_communication(self):
        """Initialize communication channels between agents"""
        # Set up message routing
        for agent in self.worker_agents:
            agent.set_message_handler(self._route_message)
        
        self.digital_twin_master.set_message_handler(self._route_message)
        print("Communication channels initialized")
    
    def _route_message(self, message):
        """Route messages between agents"""
        target_type = message.get('target_type')
        target_id = message.get('target_id')
        
        if target_type == 'master':
            self.digital_twin_master.receive_message(message)
        elif target_type == 'traffic':
            for agent in self.traffic_agents:
                if agent.agent_id == target_id:
                    agent.receive_message(message)
                    break
        elif target_type == 'weather':
            for agent in self.weather_agents:
                if agent.agent_id == target_id:
                    agent.receive_message(message)
                    break
        elif target_type == 'air_quality':
            for agent in self.air_quality_agents:
                if agent.agent_id == target_id:
                    agent.receive_message(message)
                    break
        elif target_type == 'infrastructure':
            for agent in self.infrastructure_agents:
                if agent.agent_id == target_id:
                    agent.receive_message(message)
                    break
    
    def get_digital_twin_status(self):
        """Get comprehensive status of the Digital Twin system"""
        status = {
            'system_status': self.get_system_status(),
            'agent_status': {
                'traffic_agents': len(self.traffic_agents),
                'weather_agents': len(self.weather_agents),
                'air_quality_agents': len(self.air_quality_agents),
                'infrastructure_agents': len(self.infrastructure_agents),
                'master_agent': 'active' if self.digital_twin_master else 'inactive'
            },
            'active_alerts': self._get_all_active_alerts(),
            'system_health': self._assess_system_health()
        }
        
        return status
    
    def _get_all_active_alerts(self):
        """Get all active alerts from all agents"""
        alerts = {
            'traffic_alerts': [],
            'weather_alerts': [],
            'air_quality_alerts': [],
            'infrastructure_alerts': []
        }
        
        for agent in self.traffic_agents:
            alerts['traffic_alerts'].extend(agent.congestion_alerts)
        
        for agent in self.weather_agents:
            alerts['weather_alerts'].extend(agent.weather_alerts)
        
        for agent in self.air_quality_agents:
            alerts['air_quality_alerts'].extend(agent.pollution_alerts)
        
        for agent in self.infrastructure_agents:
            alerts['infrastructure_alerts'].extend(agent.maintenance_alerts)
        
        return alerts
    
    def _assess_system_health(self):
        """Assess overall system health"""
        total_alerts = sum(len(alerts) for alerts in self._get_all_active_alerts().values())
        
        if total_alerts == 0:
            return 'excellent'
        elif total_alerts < 5:
            return 'good'
        elif total_alerts < 10:
            return 'fair'
        elif total_alerts < 20:
            return 'poor'
        else:
            return 'critical'

def main():
    """Main function to run the Digital Twin MAS system"""
    print("🌐 Digital Twin MAS System for Smart Traffic Management")
    print("=" * 70)
    
    # Create and initialize the Digital Twin MAS system
    coordinator = DigitalTwinMASCoordinator()
    
    if coordinator.initialize_digital_twin_system():
        print("\n🚀 Starting Digital Twin MAS System...")
        
        # Start the system
        if coordinator.start_system():
            print("✅ Digital Twin MAS System started successfully!")
            
            # Get system status
            status = coordinator.get_digital_twin_status()
            print(f"\n📊 System Status:")
            print(f"   Traffic Agents: {status['agent_status']['traffic_agents']}")
            print(f"   Weather Agents: {status['agent_status']['weather_agents']}")
            print(f"   Air Quality Agents: {status['agent_status']['air_quality_agents']}")
            print(f"   Infrastructure Agents: {status['agent_status']['infrastructure_agents']}")
            print(f"   Master Agent: {status['agent_status']['master_agent']}")
            print(f"   System Health: {status['system_health']}")
            
            # Keep system running for demonstration
            try:
                print("\n🔄 System running... Press Ctrl+C to stop")
                while True:
                    time.sleep(10)
                    # Get updated status
                    status = coordinator.get_digital_twin_status()
                    print(f"System Health: {status['system_health']} | Active Alerts: {sum(len(alerts) for alerts in status['active_alerts'].values())}")
            except KeyboardInterrupt:
                print("\n🛑 Stopping Digital Twin MAS System...")
                coordinator.stop_system()
                print("✅ System stopped successfully!")
        else:
            print("❌ Failed to start Digital Twin MAS System")
    else:
        print("❌ Failed to initialize Digital Twin MAS System")

if __name__ == "__main__":
    main()
