"""
Configuration registry for loading and managing YAML configs
"""
from typing import Dict, List
from pathlib import Path

from asgp.config.loader import ConfigLoader
from asgp.config.schemas import ASGPConfig, SourceConfig, AgentConfig, SourceDetails


class ConfigRegistry:
    """Central registry for all configuration"""
    
    def __init__(self):
        self.sources: Dict[str, SourceConfig] = {}
        self.agents: Dict[str, AgentConfig] = {}
        self.source_details: Dict[str, SourceDetails] = {}
    
    @classmethod
    def load(cls, config_files: List[str]) -> 'ConfigRegistry':
        """
        Load configuration from YAML file(s)
        
        Args:
            config_files: List of YAML file paths
        
        Returns:
            ConfigRegistry instance
        """
        registry = cls()
        
        for file_path in config_files:
            print(f"Loading config: {file_path}")
            config = ConfigLoader.load_file(file_path)
            registry._merge_config(config)
        
        print(f"Loaded {len(registry.sources)} sources, {len(registry.agents)} agents")
        return registry
    
    def _merge_config(self, config: ASGPConfig):
        """Merge loaded config into registry"""
        # Add sources
        for source in config.source_config:
            if source.name in self.sources:
                print(f"  Warning: Overwriting source '{source.name}'")
            self.sources[source.name] = source
        
        # Add agents
        for agent in config.agent_config:
            if agent.name in self.agents:
                print(f"  Warning: Overwriting agent '{agent.name}'")
            self.agents[agent.name] = agent
        
        # Add source details
        for name, details in config.source_details.items():
            if name in self.source_details:
                print(f"  Warning: Overwriting source_details '{name}'")
            self.source_details[name] = details
    
    def get_source(self, name: str) -> SourceConfig:
        """Get source configuration by name"""
        if name not in self.sources:
            raise KeyError(f"Source '{name}' not found in registry")
        return self.sources[name]
    
    def get_agent_config(self, name: str) -> AgentConfig:
        """Get agent configuration by name"""
        if name not in self.agents:
            raise KeyError(f"Agent '{name}' not found in registry")
        return self.agents[name]
    
    def get_source_details(self, name: str) -> SourceDetails:
        """Get source details by name"""
        if name not in self.source_details:
            raise KeyError(f"Source details '{name}' not found in registry")
        return self.source_details[name]
