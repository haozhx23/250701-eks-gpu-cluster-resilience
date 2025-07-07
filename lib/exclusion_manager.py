#!/usr/bin/env python3
"""
GPU Monitoring Instance Exclusion Manager
Used to manage exclusion list of faulty instances to prevent duplicate GPU exception handling
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

class ExclusionManager:
    def __init__(self, exclusion_file: str = "./gpu_monitor_exclusions.json", 
                 default_timeout: int = 1800):
        """
        Initialize Exclusion Manager
        
        Args:
            exclusion_file: Exclusion list file path
            default_timeout: Default exclusion timeout (seconds), default 30 minutes
        """
        self.exclusion_file = exclusion_file
        self.default_timeout = default_timeout
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup Logger"""
        logger = logging.getLogger('ExclusionManager')
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _load_exclusions(self) -> Dict:
        """Load Exclusion List"""
        if not os.path.exists(self.exclusion_file):
            return {}
        
        try:
            
            # {
            #     "i-0f7e9fd6fa5227c7c": {
            #         "node_name": "ip-10-11-141-95.ec2.internal",
            #         "error_type": "CRITICAL_UNKNOWN",
            #         "start_time": 1751642182.956038,
            #         "timeout": 3600,
            #         "readable_time": "2025-07-04 15:16:22",
            #         "expires_at": "2025-07-04 16:16:22"
            #     },
            #     "i-00cca484c3a2e151b": {
            #         "node_name": "ip-10-11-132-118.ec2.internal",
            #         "error_type": "CRITICAL_UNKNOWN",
            #         "start_time": 1751642186.693703,
            #         "timeout": 3600,
            #         "readable_time": "2025-07-04 15:16:26",
            #         "expires_at": "2025-07-04 16:16:26"
            #     }
            # }

            with open(self.exclusion_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.error(f"Failed to Load Exclusion List: {e}")
            return {}
    
    def _save_exclusions(self, exclusions: Dict) -> bool:
        """Save Exclusion List"""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.exclusion_file), exist_ok=True)
            
            with open(self.exclusion_file, 'w', encoding='utf-8') as f:
                json.dump(exclusions, f, indent=2, ensure_ascii=False)
            return True
        except IOError as e:
            self.logger.error(f"Failed to Save Exclusion List: {e}")
            return False
    
    def add_exclusion(self, instance_id: str, node_name: str, 
                     error_type: str, timeout: Optional[int] = None) -> bool:
        """
        Add Instance to Exclusion List
        
        Args:
            instance_id: Instance ID
            node_name: Node name
            error_type: Error type
            timeout: Custom timeout (seconds)
        
        Returns:
            bool: Whether successfully added
        """
        exclusions = self._load_exclusions()
        
        if instance_id in exclusions:
            self.logger.warning(f"Instance {instance_id} Already in Exclusion List")
            return False
        
        current_time = time.time()
        timeout = timeout or self.default_timeout
        
        exclusions[instance_id] = {
            'node_name': node_name,
            'error_type': error_type,
            'start_time': current_time,
            'timeout': timeout,
            'readable_time': datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S'),
            'expires_at': datetime.fromtimestamp(current_time + timeout).strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if self._save_exclusions(exclusions):
            self.logger.info(f"Added Instance {instance_id} ({node_name}) to Exclusion List - Error Type: {error_type}")
            return True
        return False
    
    def remove_exclusion(self, instance_id: str) -> bool:
        """
        Remove Instance from Exclusion List
        
        Args:
            instance_id: Instance ID
        
        Returns:
            bool: Whether successfully removed
        """
        exclusions = self._load_exclusions()
        
        if instance_id not in exclusions:
            self.logger.info(f"Instance {instance_id} Not in Exclusion List")
            return False
        
        del exclusions[instance_id]
        
        if self._save_exclusions(exclusions):
            self.logger.info(f"Removed Instance {instance_id} from Exclusion List")
            return True
        return False
    
    def is_excluded(self, instance_id: str) -> bool:
        """
        Check if Instance is in Exclusion List
        
        Args:
            instance_id: Instance ID
        
        Returns:
            bool: Whether excluded
        """
        exclusions = self._load_exclusions()
        return instance_id in exclusions
    
    def get_exclusion_info(self, instance_id: str) -> Optional[Dict]:
        """
        Get Instance Exclusion Information
        
        Args:
            instance_id: Instance ID
        
        Returns:
            Dict: Exclusion information, returns None if not exists
        """
        exclusions = self._load_exclusions()
        return exclusions.get(instance_id)
    
    def cleanup_expired(self) -> int:
        """
        Clean up Expired Exclusion Records
        
        Returns:
            int: Number of cleaned records
        """
        exclusions = self._load_exclusions()
        current_time = time.time()
        expired_instances = []
        
        for instance_id, info in exclusions.items():
            start_time = info.get('start_time', 0)
            timeout = info.get('timeout', self.default_timeout)
            
            if current_time - start_time > timeout:
                expired_instances.append(instance_id)
                node_name = info.get('node_name', 'unknown')
                elapsed = int(current_time - start_time)
                self.logger.info(f"Instance {instance_id} ({node_name}) Exclusion Time Expired ({elapsed}s), Auto Resume Monitoring")
        
        # Remove expired records
        for instance_id in expired_instances:
            del exclusions[instance_id]
        
        if expired_instances:
            self._save_exclusions(exclusions)
            self.logger.info(f"🧹 Cleaned up {len(expired_instances)} Expired Exclusion Records")
        
        return len(expired_instances)
    
    def should_monitor(self, instance_id: str, node_name: str = "") -> Tuple[bool, Optional[str]]:
        """
        Check if Specified Instance Should be Monitored
        
        Args:
            instance_id: Instance ID
            node_name: Node name (for logging)
        
        Returns:
            Tuple[bool, Optional[str]]: (Should monitor, Skip reason)
        """
        # First clean up expired records
        self.cleanup_expired()
        
        if self.is_excluded(instance_id):
            info = self.get_exclusion_info(instance_id)
            if info:
                error_type = info.get('error_type', 'unknown')
                readable_time = info.get('readable_time', 'unknown')
                reason = f"Reason: {error_type} Handling in Progress (Exclusion Time: {readable_time})"
                
                self.logger.info(f"Skip Instance {instance_id} ({node_name}) - {reason}")
                return False, reason
        
        return True, None
    
    def list_exclusions(self) -> List[Dict]:
        """
        Get All Current Exclusion Records
        
        Returns:
            List[Dict]: Exclusion records list
        """
        exclusions = self._load_exclusions()
        current_time = time.time()
        result = []
        
        for instance_id, info in exclusions.items():
            start_time = info.get('start_time', 0)
            timeout = info.get('timeout', self.default_timeout)
            elapsed = int(current_time - start_time)
            remaining = max(0, timeout - elapsed)
            
            result.append({
                'instance_id': instance_id,
                'node_name': info.get('node_name', 'unknown'),
                'error_type': info.get('error_type', 'unknown'),
                'start_time': info.get('readable_time', 'unknown'),
                'expires_at': info.get('expires_at', 'unknown'),
                'elapsed_seconds': elapsed,
                'remaining_seconds': remaining,
                'remaining_minutes': remaining // 60
            })
        
        return result
    
    def show_status(self):
        """Display Current Exclusion List Status"""
        exclusions = self.list_exclusions()
        
        print("Current GPU Monitoring Exclusion List:")
        print("=" * 80)
        
        if not exclusions:
            print("No Excluded Instances")
            return
        
        # Table header
        print(f"{'Instance ID':<20} {'Node Name':<25} {'Error Type':<15} {'Remaining Time':<10} {'Exclusion Time':<20}")
        print("-" * 80)
        
        # Data rows
        for item in exclusions:
            if item['remaining_seconds'] > 0:
                print(f"{item['instance_id']:<20} {item['node_name']:<25} "
                      f"{item['error_type']:<15} {item['remaining_minutes']}min{'':<4} {item['start_time']:<20}")
    
    def pause_instance(self, instance_id: str, node_name: str, 
                      reason: str = "MANUAL_PAUSE", timeout: Optional[int] = None) -> bool:
        """
        Manually Pause Instance Monitoring
        
        Args:
            instance_id: Instance ID
            node_name: Node name
            reason: Pause reason
            timeout: Custom timeout
        
        Returns:
            bool: Whether successfully paused
        """
        if self.add_exclusion(instance_id, node_name, reason, timeout):
            self.logger.info(f"Manually Paused Instance {instance_id} Monitoring")
            return True
        return False
    
    def resume_instance(self, instance_id: str) -> bool:
        """
        Manually Resume Instance Monitoring
        
        Args:
            instance_id: Instance ID
        
        Returns:
            bool: Whether successfully resumed
        """
        if self.remove_exclusion(instance_id):
            self.logger.info(f"Manually Resumed Instance {instance_id} Monitoring")
            return True
        return False


def main():
    """Command Line Tool Entry Point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='GPU Monitoring Exclusion Manager')
    parser.add_argument('action', choices=['add', 'remove', 'list', 'cleanup', 'check'],
                       help='Operation type')
    parser.add_argument('--instance-id', help='Instance ID')
    parser.add_argument('--node-name', help='Node name')
    parser.add_argument('--error-type', help='Error type')
    parser.add_argument('--timeout', type=int, help='Timeout (seconds)')
    
    args = parser.parse_args()
    
    manager = ExclusionManager()
    
    if args.action == 'add':
        if not args.instance_id or not args.node_name or not args.error_type:
            print("Adding exclusion record requires --instance-id, --node-name and --error-type parameters")
            return
        manager.add_exclusion(args.instance_id, args.node_name, args.error_type, args.timeout)
    
    elif args.action == 'remove':
        if not args.instance_id:
            print("Removing exclusion record requires --instance-id parameter")
            return
        manager.remove_exclusion(args.instance_id)
    
    elif args.action == 'list':
        manager.show_status()
    
    elif args.action == 'cleanup':
        count = manager.cleanup_expired()
        print(f"Cleaned up {count} expired records")
    
    elif args.action == 'check':
        if not args.instance_id:
            print("Checking instance requires --instance-id parameter")
            return
        should_monitor, reason = manager.should_monitor(args.instance_id, args.node_name or "")
        if should_monitor:
            print(f"Instance {args.instance_id} should be monitored")
        else:
            print(f"Instance {args.instance_id} should not be monitored - {reason}")


if __name__ == '__main__':
    main()
