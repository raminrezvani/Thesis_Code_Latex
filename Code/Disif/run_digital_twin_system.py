#!/usr/bin/env python3
"""
Complete Digital Twin System Execution Pipeline
Runs the entire system from data generation to MAS processing
"""

import sys
import os
import time
from pathlib import Path

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def run_complete_pipeline():
    """Run the complete Digital Twin system pipeline"""
    print("🚀 Digital Twin Smart Traffic System - Complete Pipeline")
    print("=" * 70)
    
    try:
        # Step 1: Generate Digital Twin Data
        print("\n📊 Step 1: Generating Digital Twin Data...")
        from smart_traffic_digital_twin import SmartTrafficDigitalTwin
        
        digital_twin = SmartTrafficDigitalTwin()
        all_data = digital_twin.generate_all_data(hours=24)
        digital_twin.save_all_data()
        
        print(f"✅ Generated {sum(len(data) for data in all_data.values())} total records")
        
        # Step 2: Convert Data to RDF
        print("\n🌐 Step 2: Converting Data to RDF Knowledge Graph...")
        from digital_twin_rdfizer import DigitalTwinRDFizer
        
        rdfizer = DigitalTwinRDFizer()
        rdfizer.convert_all_data()
        rdfizer.save_rdf()
        
        summary = rdfizer.get_graph_summary()
        print(f"✅ Created RDF knowledge graph with {summary['total_triples']} triples")
        
        # Step 3: Initialize and Run MAS System
        print("\n🤖 Step 3: Initializing and Running MAS System...")
        from digital_twin_mas_system import DigitalTwinMASCoordinator
        
        coordinator = DigitalTwinMASCoordinator()
        
        if coordinator.initialize_digital_twin_system():
            print("✅ Digital Twin MAS System initialized successfully!")
            
            if coordinator.start_system():
                print("✅ Digital Twin MAS System started successfully!")
                
                # Get initial system status
                status = coordinator.get_digital_twin_status()
                print(f"\n📊 Initial System Status:")
                print(f"   Traffic Agents: {status['agent_status']['traffic_agents']}")
                print(f"   Weather Agents: {status['agent_status']['weather_agents']}")
                print(f"   Air Quality Agents: {status['agent_status']['air_quality_agents']}")
                print(f"   Infrastructure Agents: {status['agent_status']['infrastructure_agents']}")
                print(f"   Master Agent: {status['agent_status']['master_agent']}")
                print(f"   System Health: {status['system_health']}")
                
                # Simulate data processing
                print("\n🔄 Simulating data processing and analysis...")
                simulate_data_processing(coordinator)
                
                # Get final system status
                final_status = coordinator.get_digital_twin_status()
                print(f"\n📊 Final System Status:")
                print(f"   System Health: {final_status['system_health']}")
                print(f"   Active Alerts: {sum(len(alerts) for alerts in final_status['active_alerts'].values())}")
                
                # Stop the system
                print("\n🛑 Stopping Digital Twin MAS System...")
                coordinator.stop_system()
                print("✅ System stopped successfully!")
                
            else:
                print("❌ Failed to start Digital Twin MAS System")
        else:
            print("❌ Failed to initialize Digital Twin MAS System")
        
        print("\n🎯 Complete Pipeline Execution Finished!")
        print("📁 Check the 'output' directory for generated files:")
        
        output_dir = Path("output")
        if output_dir.exists():
            for file in output_dir.iterdir():
                if file.is_file():
                    size = file.stat().st_size
                    print(f"   📄 {file.name} ({size} bytes)")
        
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        print("Make sure all required modules are available")
    except Exception as e:
        print(f"❌ Error during pipeline execution: {e}")
        import traceback
        traceback.print_exc()

def simulate_data_processing(coordinator):
    """Simulate data processing by the MAS system"""
    print("   Simulating traffic data processing...")
    time.sleep(2)
    
    print("   Simulating weather data analysis...")
    time.sleep(2)
    
    print("   Simulating air quality monitoring...")
    time.sleep(2)
    
    print("   Simulating infrastructure health assessment...")
    time.sleep(2)
    
    print("   Simulating cross-domain correlation analysis...")
    time.sleep(2)
    
    print("   Simulating intelligent decision making...")
    time.sleep(2)
    
    print("   ✅ Data processing simulation completed!")

def run_individual_components():
    """Run individual components separately"""
    print("🔧 Digital Twin System - Individual Components")
    print("=" * 50)
    
    while True:
        print("\nSelect component to run:")
        print("1. Generate Digital Twin Data")
        print("2. Convert Data to RDF")
        print("3. Run MAS System")
        print("4. Run Complete Pipeline")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == "1":
            print("\n📊 Generating Digital Twin Data...")
            from smart_traffic_digital_twin import SmartTrafficDigitalTwin
            digital_twin = SmartTrafficDigitalTwin()
            all_data = digital_twin.generate_all_data(hours=24)
            digital_twin.save_all_data()
            print(f"✅ Generated {sum(len(data) for data in all_data.values())} total records")
            
        elif choice == "2":
            print("\n🌐 Converting Data to RDF...")
            from digital_twin_rdfizer import DigitalTwinRDFizer
            rdfizer = DigitalTwinRDFizer()
            rdfizer.convert_all_data()
            rdfizer.save_rdf()
            summary = rdfizer.get_graph_summary()
            print(f"✅ Created RDF knowledge graph with {summary['total_triples']} triples")
            
        elif choice == "3":
            print("\n🤖 Running MAS System...")
            from digital_twin_mas_system import DigitalTwinMASCoordinator
            coordinator = DigitalTwinMASCoordinator()
            
            if coordinator.initialize_digital_twin_system():
                if coordinator.start_system():
                    print("✅ MAS System started successfully!")
                    
                    # Keep running for a while
                    try:
                        for i in range(5):
                            time.sleep(2)
                            status = coordinator.get_digital_twin_status()
                            print(f"System Health: {status['system_health']} | Active Alerts: {sum(len(alerts) for alerts in status['active_alerts'].values())}")
                    except KeyboardInterrupt:
                        pass
                    
                    coordinator.stop_system()
                    print("✅ MAS System stopped")
                else:
                    print("❌ Failed to start MAS System")
            else:
                print("❌ Failed to initialize MAS System")
                
        elif choice == "4":
            run_complete_pipeline()
            
        elif choice == "5":
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please enter 1-5.")

def main():
    """Main function"""
    print("🚗 Smart Traffic Digital Twin System")
    print("=" * 50)
    
    while True:
        print("\nSelect execution mode:")
        print("1. Run Complete Pipeline")
        print("2. Run Individual Components")
        print("3. Exit")
        
        choice = input("\nEnter your choice (1-3): ").strip()
        
        if choice == "1":
            run_complete_pipeline()
            break
        elif choice == "2":
            run_individual_components()
            break
        elif choice == "3":
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please enter 1-3.")

if __name__ == "__main__":
    main()
