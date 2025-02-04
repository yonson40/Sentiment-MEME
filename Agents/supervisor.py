import logging
from datetime import datetime, timedelta
from typing import Dict, List
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from langgraph.graph import StateGraph, END

from .schema import AgentState
from .agents import OHLCVAgent, TweetAgent, SentimentAgent, should_continue

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Supervisor:
    def __init__(self):
        self.workflow = self._create_workflow()
        self.scheduler = BlockingScheduler()
     
    def _create_workflow(self) -> StateGraph:
        """Create the agent workflow graph"""
        # Initialize our agents
        ohlcv_agent = OHLCVAgent()
        tweet_agent = TweetAgent()
        sentiment_agent = SentimentAgent()
        
        # Create the graph
        workflow = StateGraph(AgentState)
        
        # Add our agent nodes
        workflow.add_node('ohlcv_agent', ohlcv_agent)
        workflow.add_node('tweet_agent', tweet_agent)
        workflow.add_node('sentiment_agent', sentiment_agent)
        
        # Add edges - the should_continue function will determine the next node
        workflow.add_edge('ohlcv_agent', should_continue)
        workflow.add_edge('tweet_agent', should_continue)
        workflow.add_edge('sentiment_agent', should_continue)
        
        # Set the entry point
        workflow.set_entry_point('ohlcv_agent')
        
        return workflow.compile()
    
    def execute_workflow(self):
        """Execute the agent workflow"""
        try:
            logger.info("Starting agent workflow execution")
            
            # Initialize state
            state = AgentState(
                status="initialized",
                last_run=datetime.now()
            )
            
            # Run the workflow
            final_state = self.workflow.invoke(state)
            
            # Log results
            logger.info(f"Workflow completed with status: {final_state.status}")
            if final_state.error_messages:
                logger.error("Errors encountered:")
                for error in final_state.error_messages:
                    logger.error(error)
            
            logger.info(f"OHLCV updates: {len(final_state.ohlcv_updates)}")
            logger.info(f"New tweets: {len(final_state.new_tweets)}")
            logger.info(f"Sentiment updates: {len(final_state.sentiment_scores)}")
            logger.info(f"Token sentiment updates: {len(final_state.token_sentiments)}")
            
        except Exception as e:
            logger.error(f"Error in workflow execution: {str(e)}")
            raise

    def start(self):
        """Start the supervisor with scheduled tasks"""
        # Schedule OHLCV and tweet collection every 15 minutes
        self.scheduler.add_job(
            self.execute_workflow,
            'interval',
            minutes=15,
            next_run_time=datetime.now()
        )
        
        logger.info("Supervisor started. Workflow scheduled.")
        
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Supervisor stopped.")

if __name__ == "__main__":
    supervisor = Supervisor()
    supervisor.start()