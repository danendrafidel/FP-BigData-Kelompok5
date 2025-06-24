#!/usr/bin/env python3
"""
Script sederhana untuk test flag system
"""

import requests
import time

def test_flags():
    """Test flag endpoints"""
    base_url = "http://localhost:4000"
    
    # Test flags yang penting
    test_flags = [
        'id.svg',  # Indonesia
        'us.svg',  # United States
        'sg.svg',  # Singapore
        'my.svg',  # Malaysia
        'in.svg',  # India
        'au.svg',  # Australia
        'jp.svg',  # Japan
        'de.svg',  # Germany
        'gb.svg',  # United Kingdom
        'ca.svg',  # Canada
    ]
    
    print("🧪 Testing Flag System")
    print("=" * 30)
    
    success = 0
    failed = 0
    
    for flag in test_flags:
        url = f"{base_url}/flag/{flag}"
        print(f"Testing: {flag}")
        
        try:
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                print(f"   ✅ SUCCESS - {len(response.content)} bytes")
                success += 1
            else:
                print(f"   ❌ FAILED - HTTP {response.status_code}")
                failed += 1
                
        except requests.exceptions.ConnectionError:
            print(f"   ❌ FAILED - Flask app tidak running")
            failed += 1
        except Exception as e:
            print(f"   ❌ FAILED - {str(e)}")
            failed += 1
        
        time.sleep(0.1)
    
    print(f"\n📊 Results:")
    print(f"   ✅ Success: {success}")
    print(f"   ❌ Failed: {failed}")
    
    if success > 0:
        print(f"\n🎉 Flag system working!")
        print(f"🌐 Flags ready for job recommendations")
    else:
        print(f"\n❌ Flag system not working")
        print(f"💡 Make sure:")
        print(f"   • python setup_flags.py (sudah dijalankan)")
        print(f"   • python app.py (Flask app running)")

if __name__ == "__main__":
    test_flags() 