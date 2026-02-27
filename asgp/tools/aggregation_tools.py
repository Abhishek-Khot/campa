
"""
MongoDB Aggregation Tools - High-level aggregation operations
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class AggregationTool:
    """Base class for aggregation operations"""
    name: str
    description: str
    parameters: Dict[str, Any]
    
    def build_pipeline(self) -> List[Dict[str, Any]]:
        """Build MongoDB aggregation pipeline"""
        raise NotImplementedError


class CalculateAverage(AggregationTool):
    """Calculate average of a numeric field"""
    
    def __init__(self, field: str, filter_dict: Optional[Dict] = None):
        super().__init__(
            name="calculate_average",
            description=f"Calculate average of {field}",
            parameters={"field": field, "filter": filter_dict}
        )
        self.field = field
        self.filter_dict = filter_dict or {}
    
    def build_pipeline(self) -> List[Dict[str, Any]]:
        pipeline = []
        if self.filter_dict:
            pipeline.append({"$match": self.filter_dict})
        pipeline.append({
            "$group": {
                "_id": None,
                f"avg_{self.field}": {"$avg": f"${self.field}"}
            }
        })
        return pipeline


class CalculateSum(AggregationTool):
    """Calculate sum of a numeric field"""
    
    def __init__(self, field: str, filter_dict: Optional[Dict] = None):
        super().__init__(
            name="calculate_sum",
            description=f"Calculate sum of {field}",
            parameters={"field": field, "filter": filter_dict}
        )
        self.field = field
        self.filter_dict = filter_dict or {}
    
    def build_pipeline(self) -> List[Dict[str, Any]]:
        pipeline = []
        if self.filter_dict:
            pipeline.append({"$match": self.filter_dict})
        pipeline.append({
            "$group": {
                "_id": None,
                f"total_{self.field}": {"$sum": f"${self.field}"}
            }
        })
        return pipeline


class CountDocuments(AggregationTool):
    """Count documents matching filter"""
    
    def __init__(self, filter_dict: Optional[Dict] = None):
        super().__init__(
            name="count_documents",
            description="Count documents",
            parameters={"filter": filter_dict}
        )
        self.filter_dict = filter_dict or {}
    
    def build_pipeline(self) -> List[Dict[str, Any]]:
        pipeline = []
        if self.filter_dict:
            pipeline.append({"$match": self.filter_dict})
        pipeline.append({"$count": "total_count"})
        return pipeline


class FindMinMax(AggregationTool):
    """Find min and max values of a field"""
    
    def __init__(self, field: str, filter_dict: Optional[Dict] = None):
        super().__init__(
            name="find_min_max",
            description=f"Find min/max of {field}",
            parameters={"field": field, "filter": filter_dict}
        )
        self.field = field
        self.filter_dict = filter_dict or {}
    
    def build_pipeline(self) -> List[Dict[str, Any]]:
        pipeline = []
        if self.filter_dict:
            pipeline.append({"$match": self.filter_dict})
        pipeline.append({
            "$group": {
                "_id": None,
                f"min_{self.field}": {"$min": f"${self.field}"},
                f"max_{self.field}": {"$max": f"${self.field}"}
            }
        })
        return pipeline


class GroupByAggregate(AggregationTool):
    """Group by field with multiple aggregations"""
    
    def __init__(
        self,
        group_field: str,
        aggregations: Dict[str, Dict[str, str]],  # {"avg_price": {"$avg": "price"}}
        filter_dict: Optional[Dict] = None,
        sort_by: Optional[str] = None,
        limit: Optional[int] = None
    ):
        super().__init__(
            name="group_by_aggregate",
            description=f"Group by {group_field}",
            parameters={
                "group_field": group_field,
                "aggregations": aggregations,
                "filter": filter_dict,
                "sort_by": sort_by,
                "limit": limit
            }
        )
        self.group_field = group_field
        self.aggregations = aggregations
        self.filter_dict = filter_dict or {}
        self.sort_by = sort_by
        self.limit = limit
    
    def build_pipeline(self) -> List[Dict[str, Any]]:
        pipeline = []
        
        # Match stage
        if self.filter_dict:
            pipeline.append({"$match": self.filter_dict})
        
        # Group stage
        group_stage = {"_id": f"${self.group_field}"}
        for result_name, agg_spec in self.aggregations.items():
            group_stage[result_name] = agg_spec
        pipeline.append({"$group": group_stage})
        
        # Sort stage
        if self.sort_by:
            pipeline.append({"$sort": {self.sort_by: -1}})
        
        # Limit stage
        if self.limit:
            pipeline.append({"$limit": self.limit})
        
        return pipeline


class TopN(AggregationTool):
    """Get top N documents sorted by field"""
    
    def __init__(
        self,
        sort_field: str,
        n: int,
        filter_dict: Optional[Dict] = None,
        ascending: bool = False
    ):
        super().__init__(
            name="top_n",
            description=f"Get top {n} by {sort_field}",
            parameters={
                "sort_field": sort_field,
                "n": n,
                "filter": filter_dict,
                "ascending": ascending
            }
        )
        self.sort_field = sort_field
        self.n = n
        self.filter_dict = filter_dict or {}
        self.ascending = ascending
    
    def build_pipeline(self) -> List[Dict[str, Any]]:
        pipeline = []
        if self.filter_dict:
            pipeline.append({"$match": self.filter_dict})
        pipeline.append({"$sort": {self.sort_field: 1 if self.ascending else -1}})
        pipeline.append({"$limit": self.n})
        return pipeline


# Tool registry
AGGREGATION_TOOLS = {
    "calculate_average": CalculateAverage,
    "calculate_sum": CalculateSum,
    "count_documents": CountDocuments,
    "find_min_max": FindMinMax,
    "group_by_aggregate": GroupByAggregate,
    "top_n": TopN
}


def get_tool_descriptions() -> str:
    """Get formatted tool descriptions for LLM"""
    descriptions = []
    descriptions.append("AVAILABLE AGGREGATION TOOLS:\n")
    
    descriptions.append("""
1. calculate_average
   - Calculate average of a numeric field
   - Parameters: field (required), filter (optional)
   - Example: {"tool": "calculate_average", "field": "price", "filter": {"category": "electronics"}}

2. calculate_sum
   - Calculate sum of a numeric field
   - Parameters: field (required), filter (optional)
   - Example: {"tool": "calculate_sum", "field": "price"}

3. count_documents
   - Count documents matching filter
   - Parameters: filter (optional)
   - Example: {"tool": "count_documents", "filter": {"category": "electronics"}}

4. find_min_max
   - Find minimum and maximum values
   - Parameters: field (required), filter (optional)
   - Example: {"tool": "find_min_max", "field": "price"}

5. group_by_aggregate
   - Group by field with custom aggregations
   - Parameters: group_field, aggregations, filter (optional), sort_by, limit
   - Example: {
       "tool": "group_by_aggregate",
       "group_field": "category",
       "aggregations": {
         "avg_price": {"$avg": "$price"},
         "count": {"$sum": 1}
       },
       "sort_by": "avg_price",
       "limit": 10
     }

6. top_n
   - Get top N documents sorted by field
   - Parameters: sort_field, n, filter (optional), ascending (default: false)
   - Example: {"tool": "top_n", "sort_field": "price", "n": 5}
""")
    
    return "\n".join(descriptions)