"""Simple DAG framework for pipeline orchestration."""

from typing import List, Dict, Optional, Callable, Set
from enum import Enum
from datetime import datetime
import logging

logger = logging.getLogger("nyc_taxi_etl")


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class Task:
    """Represents a task in the DAG."""
    
    def __init__(
        self,
        task_id: str,
        task_function: Callable,
        dependencies: Optional[List[str]] = None,
        retries: int = 0,
        retry_delay_seconds: int = 60,
        description: str = ""
    ):
        """
        Initialize a task.
        
        Args:
            task_id: Unique identifier for the task
            task_function: Function to execute
            dependencies: List of task IDs this task depends on
            retries: Number of retry attempts on failure
            retry_delay_seconds: Delay between retries
            description: Task description
        """
        self.task_id = task_id
        self.task_function = task_function
        self.dependencies = dependencies or []
        self.retries = retries
        self.retry_delay_seconds = retry_delay_seconds
        self.description = description
        
        self.status = TaskStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.error: Optional[str] = None
        self.attempts = 0
    
    def can_run(self, completed_tasks: Set[str]) -> bool:
        """
        Check if task can run (all dependencies completed).
        
        Args:
            completed_tasks: Set of completed task IDs
        
        Returns:
            True if all dependencies are completed
        """
        return all(dep in completed_tasks for dep in self.dependencies)
    
    def execute(self, *args, **kwargs) -> Dict:
        """
        Execute the task.
        
        Args:
            *args: Positional arguments for task function
            **kwargs: Keyword arguments for task function
        
        Returns:
            Dictionary with execution results
        """
        self.status = TaskStatus.RUNNING
        self.start_time = datetime.now()
        self.attempts += 1
        
        try:
            logger.info(f"Executing task: {self.task_id} (attempt {self.attempts})")
            result = self.task_function(*args, **kwargs)
            self.status = TaskStatus.SUCCESS
            self.end_time = datetime.now()
            
            duration = (self.end_time - self.start_time).total_seconds()
            logger.info(f"Task {self.task_id} completed successfully in {duration:.2f} seconds")
            
            return {
                "task_id": self.task_id,
                "status": "success",
                "duration_seconds": duration,
                "result": result
            }
            
        except Exception as e:
            self.error = str(e)
            duration = (datetime.now() - self.start_time).total_seconds()
            logger.error(f"Task {self.task_id} failed after {duration:.2f} seconds: {str(e)}")
            
            if self.attempts <= self.retries:
                logger.info(f"Retrying task {self.task_id} (attempt {self.attempts}/{self.retries})")
                import time
                time.sleep(self.retry_delay_seconds)
                return self.execute(*args, **kwargs)
            else:
                self.status = TaskStatus.FAILED
                self.end_time = datetime.now()
                return {
                    "task_id": self.task_id,
                    "status": "failed",
                    "duration_seconds": duration,
                    "error": str(e),
                    "attempts": self.attempts
                }


class DAG:
    """Directed Acyclic Graph for task orchestration."""
    
    def __init__(self, dag_id: str, description: str = ""):
        """
        Initialize a DAG.
        
        Args:
            dag_id: Unique identifier for the DAG
            description: DAG description
        """
        self.dag_id = dag_id
        self.description = description
        self.tasks: Dict[str, Task] = {}
        self.execution_order: List[str] = []
    
    def add_task(self, task: Task) -> None:
        """
        Add a task to the DAG.
        
        Args:
            task: Task instance
        """
        self.tasks[task.task_id] = task
    
    def _topological_sort(self) -> List[str]:
        """
        Perform topological sort to determine execution order.
        
        Returns:
            List of task IDs in execution order
        """
        # Kahn's algorithm for topological sort
        in_degree = {task_id: 0 for task_id in self.tasks}
        
        # Calculate in-degrees
        for task in self.tasks.values():
            for dep in task.dependencies:
                if dep in self.tasks:
                    in_degree[task.task_id] += 1
        
        # Find tasks with no dependencies
        queue = [task_id for task_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            task_id = queue.pop(0)
            result.append(task_id)
            
            # Reduce in-degree for dependent tasks
            for task in self.tasks.values():
                if task_id in task.dependencies:
                    in_degree[task.task_id] -= 1
                    if in_degree[task.task_id] == 0:
                        queue.append(task.task_id)
        
        # Check for cycles
        if len(result) != len(self.tasks):
            raise ValueError("DAG contains cycles or missing dependencies")
        
        return result
    
    def execute(self, *args, **kwargs) -> Dict:
        """
        Execute all tasks in the DAG in topological order.
        
        Args:
            *args: Positional arguments to pass to tasks
            **kwargs: Keyword arguments to pass to tasks
        
        Returns:
            Dictionary with execution summary
        """
        logger.info(f"Executing DAG: {self.dag_id}")
        start_time = datetime.now()
        
        # Determine execution order
        self.execution_order = self._topological_sort()
        logger.info(f"Execution order: {self.execution_order}")
        
        completed_tasks: Set[str] = set()
        task_results = {}
        
        for task_id in self.execution_order:
            task = self.tasks[task_id]
            
            if not task.can_run(completed_tasks):
                logger.error(f"Task {task_id} cannot run - dependencies not met")
                task.status = TaskStatus.FAILED
                task_results[task_id] = {
                    "status": "failed",
                    "error": "Dependencies not met"
                }
                continue
            
            # Execute task
            result = task.execute(*args, **kwargs)
            task_results[task_id] = result
            
            if result["status"] == "success":
                completed_tasks.add(task_id)
            else:
                logger.error(f"Task {task_id} failed, stopping DAG execution")
                break
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Summary
        successful = sum(1 for r in task_results.values() if r.get("status") == "success")
        failed = sum(1 for r in task_results.values() if r.get("status") == "failed")
        
        summary = {
            "dag_id": self.dag_id,
            "status": "success" if failed == 0 else "failed",
            "total_tasks": len(self.tasks),
            "successful_tasks": successful,
            "failed_tasks": failed,
            "duration_seconds": duration,
            "task_results": task_results
        }
        
        logger.info(f"DAG execution completed: {successful}/{len(self.tasks)} tasks successful")
        return summary

