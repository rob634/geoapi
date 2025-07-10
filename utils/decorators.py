import time
from functools import wraps
import inspect
from typing import Dict, Any, Optional, Union, List, Callable
from utils import logger

def validate_raster_workflow(
    inputs: Dict[str, Dict[str, Any]] = None,
    outputs: Dict[str, Dict[str, Any]] = None,
    performance_logging: bool = True,
    detailed_validation: bool = True
):
    """
    Advanced decorator for comprehensive raster workflow validation.
    
    Args:
        inputs: Dict of input validations, e.g.:
                {
                    'source_raster': {
                        'param_name': 'raster_name_in',
                        'container_param': 'container_name',
                        'required': True,
                        'validate_crs': True,
                        'min_size_mb': 0.1
                    }
                }
        outputs: Dict of output validations, e.g.:
                {
                    'result_raster': {
                        'param_name': 'raster_name_out',
                        'container_param': 'output_container_name',
                        'source': 'return_value',  # or 'result_dict_key'
                        'required_extensions': ['.tif'],
                        'min_size_mb': 0.1,
                        'validate_crs': True,
                        'compare_crs_to_input': 'source_raster'
                    }
                }
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            start_time = time.time()
            function_name = f"{self.__class__.__name__}.{func.__name__}"
            
            logger.info(f"🔍 Starting validation for {function_name}")
            
            # Get function signature for parameter mapping
            sig = inspect.signature(func)
            bound_args = sig.bind(self, *args, **kwargs)
            bound_args.apply_defaults()
            
            # Store input metadata for comparison
            input_metadata = {}
            
            # === PRE-EXECUTION VALIDATION ===
            if inputs:
                logger.debug(f"📥 Pre-validation: Checking {len(inputs)} input(s)")
                
                for input_name, config in inputs.items():
                    logger.debug(f"  Validating input: {input_name}")
                    
                    # Get raster name and container
                    raster_param = config['param_name']
                    container_param = config.get('container_param', 'container_name')
                    raster_name = bound_args.arguments.get(raster_param)
                    container_name = bound_args.arguments.get(container_param)
                    
                    # Handle missing inputs
                    if not raster_name:
                        if config.get('required', True):
                            raise ValueError(f"Required input {raster_param} not provided for {function_name}")
                        else:
                            logger.warning(f"Optional input {raster_param} not provided")
                            continue
                    
                    # Default container fallback
                    if not container_name:
                        container_name = getattr(self, 'workspace_container_name', None)
                        if not container_name and config.get('required', True):
                            raise ValueError(f"No container specified for {input_name}")
                    
                    # Validate raster name format
                    if hasattr(self, 'valid_raster_name'):
                        if not getattr(self, 'valid_raster_name')(raster_name):
                            raise ValueError(f"Invalid raster name format: {raster_name}")
                    
                    # Check existence
                    if hasattr(self, 'blob_exists'):
                        if not self.blob_exists(blob_name=raster_name, container_name=container_name):
                            raise FileNotFoundError(f"Input raster not found: {raster_name} in {container_name}")
                    
                    # Detailed validation
                    if detailed_validation:
                        metadata = _validate_raster_details(
                            raster_name, container_name, config, input_name
                        )
                        input_metadata[input_name] = metadata
                    
                    logger.info(f"  ✓ {input_name}: {raster_name} validated")
            
            # === EXECUTE FUNCTION ===
            logger.debug(f"⚙️  Executing {function_name}")
            execution_start = time.time()
            
            try:
                result = func(self, *args, **kwargs)
                execution_time = time.time() - execution_start
                logger.debug(f"✓ Function executed in {execution_time:.2f}s")
            except Exception as e:
                logger.error(f"❌ Function {function_name} failed: {e}")
                raise
            
            # === POST-EXECUTION VALIDATION ===
            if outputs:
                logger.debug(f"📤 Post-validation: Checking {len(outputs)} output(s)")
                
                for output_name, config in outputs.items():
                    logger.debug(f"  Validating output: {output_name}")
                    
                    # Get output raster name
                    output_raster = _extract_output_raster(result, config, bound_args)
                    if not output_raster:
                        if config.get('required', True):
                            raise ValueError(f"Required output {output_name} not found")
                        continue
                    
                    # Get output container
                    container_param = config.get('container_param', 'output_container_name')
                    output_container = bound_args.arguments.get(container_param)
                    if not output_container:
                        output_container = getattr(self, 'output_container_name', None)
                        if not output_container:
                            output_container = bound_args.arguments.get('container_name')
                    
                    # Validate output exists
                    if hasattr(self, 'blob_exists'):
                        if not self.blob_exists(blob_name=output_raster, container_name=output_container):
                            raise RuntimeError(f"Output not created: {output_raster} in {output_container}")
                    
                    # Detailed output validation
                    if detailed_validation:
                        _validate_output_details(
                            output_raster, output_container, config, output_name, input_metadata
                        )
                    
                    logger.info(f"  ✓ {output_name}: {output_raster} validated")
            
            # === PERFORMANCE LOGGING ===
            if performance_logging:
                total_time = time.time() - start_time
                logger.info(f"🎯 {function_name} completed in {total_time:.2f}s")
                
                if hasattr(self, '_log_performance_metrics'):
                    getattr(self, '_log_performance_metrics')(function_name, total_time, execution_time)
            
            logger.info(f"✅ All validations passed for {function_name}")
            return result
            
        return wrapper
    return decorator

# Helper methods to add to your RasterHandler class
def _validate_raster_details(self, raster_name: str, container_name: str, 
                           config: Dict[str, Any], input_name: str) -> Dict[str, Any]:
    """Detailed validation of raster properties"""
    metadata = {}
    
    try:
        # Size validation
        if config.get('min_size_mb'):
            if hasattr(self, 'get_blob_properties'):
                props = getattr(self, 'get_blob_properties')(blob_name=raster_name, container_name=container_name)
                size_mb = props.size / (1024 * 1024)
                metadata['size_mb'] = size_mb
                
                if size_mb < config['min_size_mb']:
                    raise ValueError(f"Input {input_name} too small: {size_mb:.2f}MB < {config['min_size_mb']}MB")
        
        # CRS validation
        if config.get('validate_crs'):
            if hasattr(self, 'get_epsg_code'):
                try:
                    epsg_code = self.get_epsg_code(raster_name=raster_name, container_name=container_name)
                    metadata['epsg_code'] = epsg_code
                    logger.debug(f"    CRS: EPSG:{epsg_code}")
                except Exception as e:
                    logger.warning(f"    Could not read CRS: {e}")
        
        return metadata
        
    except Exception as e:
        logger.error(f"Detailed validation failed for {input_name}: {e}")
        raise

def _extract_output_raster(self, result: Any, config: Dict[str, Any], bound_args) -> Optional[str]:
    """Extract output raster name from function result"""
    source = config.get('source', 'return_value')
    
    if source == 'return_value':
        if isinstance(result, str):
            return result
        elif isinstance(result, dict) and config.get('param_name') in result:
            return result[config['param_name']]
    elif isinstance(result, dict) and source in result:
        return result[source]
    else:
        # Try to get from bound arguments
        return bound_args.arguments.get(config.get('param_name'))
    
    return None

def _validate_output_details(self, output_raster: str, output_container: str,
                           config: Dict[str, Any], output_name: str, 
                           input_metadata: Dict[str, Any]) -> None:
    """Detailed validation of output properties"""
    
    # Extension validation
    required_extensions = config.get('required_extensions', ['.tif', '.tiff'])
    if required_extensions:
        valid_ext = any(output_raster.lower().endswith(ext) for ext in required_extensions)
        if not valid_ext:
            raise ValueError(f"Output {output_name} has invalid extension: {output_raster}")
    
    # Size validation
    if config.get('min_size_mb') and hasattr(self, 'get_blob_properties'):
        props = getattr(self, 'get_blob_properties')(
            blob_name=output_raster, container_name=output_container)
        size_mb = props.size / (1024 * 1024)
        
        if size_mb < config['min_size_mb']:
            raise RuntimeError(f"Output {output_name} too small: {size_mb:.2f}MB")
        
        logger.debug(f"    Size: {size_mb:.2f}MB")
    
    # CRS comparison
    if config.get('compare_crs_to_input') and hasattr(self, 'get_epsg_code'):
        input_ref = config['compare_crs_to_input']
        if input_ref in input_metadata and 'epsg_code' in input_metadata[input_ref]:
            try:
                output_epsg = getattr(self, 'get_epsg_code')(raster_name=output_raster, container_name=output_container)
                input_epsg = input_metadata[input_ref]['epsg_code']
                
                if config.get('expect_crs_change', True):
                    if output_epsg == input_epsg:
                        logger.warning(f"Output CRS unchanged: EPSG:{output_epsg}")
                else:
                    if output_epsg != input_epsg:
                        raise RuntimeError(f"Unexpected CRS change: EPSG:{input_epsg} -> EPSG:{output_epsg}")
                
                logger.debug(f"    CRS: EPSG:{output_epsg}")
            except Exception as e:
                logger.warning(f"CRS comparison failed: {e}")