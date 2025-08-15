from sqlalchemy.orm import Session
from models.freelance_management import InvoiceTemplate, Customer
from typing import Optional, List

class InvoiceTemplateService:
    """
    Service class for managing invoice templates with proper default handling.
    """
    
    @staticmethod
    def create_template(session: Session, user_id: str, customer_id: Optional[str], 
                       name: str, template_data: dict, is_default: bool = False) -> InvoiceTemplate:
        """
        Create a new invoice template with proper default handling.
        
        Args:
            session: Database session
            user_id: ID of the user creating the template
            customer_id: ID of the customer (optional)
            name: Template name
            template_data: Template configuration data
            is_default: Whether this should be the default template for the customer
            
        Returns:
            InvoiceTemplate: The created template
        """
        template = InvoiceTemplate(
            user_id=user_id,
            customer_id=customer_id,
            name=name,
            template_data=template_data,
            is_customer_default=False  # Will be set by make_default_for_customer if needed
        )
        
        session.add(template)
        session.flush()  # Get the ID
        
        if is_default and customer_id:
            template.make_default_for_customer(session)
        
        session.commit()
        return template
    
    @staticmethod
    def set_as_default(session: Session, template_id: str) -> bool:
        """
        Set a template as the default for its customer.
        
        Args:
            session: Database session
            template_id: ID of the template to set as default
            
        Returns:
            bool: True if successful, False if template not found
        """
        template = session.query(InvoiceTemplate).filter(
            InvoiceTemplate.id == template_id
        ).first()
        
        if not template or not template.customer_id:
            return False
            
        template.make_default_for_customer(session)
        session.commit()
        return True
    
    @staticmethod
    def remove_default(session: Session, template_id: str) -> bool:
        """
        Remove default status from a template.
        
        Args:
            session: Database session
            template_id: ID of the template to remove default status from
            
        Returns:
            bool: True if successful, False if template not found
        """
        template = session.query(InvoiceTemplate).filter(
            InvoiceTemplate.id == template_id
        ).first()
        
        if not template:
            return False
            
        template.remove_default_status()
        session.commit()
        return True
    
    @staticmethod
    def get_customer_templates(session: Session, customer_id: str) -> List[InvoiceTemplate]:
        """
        Get all templates for a specific customer.
        
        Args:
            session: Database session
            customer_id: ID of the customer
            
        Returns:
            List[InvoiceTemplate]: List of templates for the customer
        """
        return session.query(InvoiceTemplate).filter(
            InvoiceTemplate.customer_id == customer_id
        ).order_by(InvoiceTemplate.is_customer_default.desc(), InvoiceTemplate.name).all()
    
    @staticmethod
    def get_default_template(session: Session, customer_id: str) -> Optional[InvoiceTemplate]:
        """
        Get the default template for a customer.
        
        Args:
            session: Database session
            customer_id: ID of the customer
            
        Returns:
            Optional[InvoiceTemplate]: The default template if exists, None otherwise
        """
        return session.query(InvoiceTemplate).filter(
            InvoiceTemplate.customer_id == customer_id,
            InvoiceTemplate.is_customer_default == True
        ).first()
    
    @staticmethod
    def update_template(session: Session, template_id: str, name: Optional[str] = None,
                       template_data: Optional[dict] = None, is_default: Optional[bool] = None) -> bool:
        """
        Update an existing template.
        
        Args:
            session: Database session
            template_id: ID of the template to update
            name: New name (optional)
            template_data: New template data (optional)
            is_default: Whether to set as default (optional)
            
        Returns:
            bool: True if successful, False if template not found
        """
        template = session.query(InvoiceTemplate).filter(
            InvoiceTemplate.id == template_id
        ).first()
        
        if not template:
            return False
        
        # Handle name update (preserve default suffix if it exists)
        if name is not None:
            is_currently_default = template.name.endswith(" (default)")
            if is_currently_default:
                template.name = f"{name} (default)"
            else:
                template.name = name
        
        if template_data is not None:
            template.template_data = template_data
        
        # Handle default status change
        if is_default is not None:
            if is_default and template.customer_id:
                template.make_default_for_customer(session)
            elif not is_default:
                template.remove_default_status()
        
        session.commit()
        return True
