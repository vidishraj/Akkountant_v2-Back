from flask import g
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from models.freelance_management import InvoiceTemplate, Customer
from services.Base_Service import BaseService
from utils.logger import Logger


class TemplateService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TemplateService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()

    def create_template(self, template_data):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            is_default = template_data.get('isCustomerDefault', False)
            customer_id = template_data.get('customerId')

            template = InvoiceTemplate(
                user_id=user_id,
                customer_id=customer_id,
                name=template_data['name'],
                template_data=template_data['templateData'],  # Handle camelCase
                is_customer_default=False  # Will be set by make_default_for_customer if needed
            )

            self.db.session.add(template)
            self.db.session.flush()  # Get the ID for the template
            
            # If this template should be default for a customer, handle the default logic
            if is_default and customer_id:
                template.make_default_for_customer(self.db.session)
            elif is_default and not customer_id:
                # If it's marked as default but has no customer, just set the flag
                template.is_customer_default = True

            self.db.session.commit()
            
            self.logger.info(f"Template created successfully: {template.id}")
            return self._format_template(template)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error creating template: {str(e)}")
            raise

    def get_templates(self):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            templates = self.db.session.query(InvoiceTemplate).filter_by(user_id=user_id).all()
            return [self._format_template(template) for template in templates]

        except Exception as e:
            self.logger.error(f"Error fetching templates: {str(e)}")
            raise

    def get_template_by_id(self, template_id):
        try:
            user_id = g.get('firebase_id')
            template = self.db.session.query(InvoiceTemplate).filter_by(
                id=template_id, 
                user_id=user_id
            ).first()

            if not template:
                raise ValueError("Template not found")

            return self._format_template(template)

        except Exception as e:
            self.logger.error(f"Error fetching template: {str(e)}")
            raise

    def update_template(self, template_id, template_data):
        try:
            user_id = g.get('firebase_id')
            template = self.db.session.query(InvoiceTemplate).filter_by(
                id=template_id, 
                user_id=user_id
            ).first()

            if not template:
                raise ValueError("Template not found")

            # Update fields (handle both camelCase and snake_case)
            if 'name' in template_data:
                template.name = template_data['name']
            if 'templateData' in template_data:
                template.template_data = template_data['templateData']
            elif 'template_data' in template_data:
                template.template_data = template_data['template_data']
            if 'isCustomerDefault' in template_data:
                template.is_customer_default = template_data['isCustomerDefault']
            elif 'is_customer_default' in template_data:
                template.is_customer_default = template_data['is_customer_default']
            if 'customerId' in template_data:
                template.customer_id = template_data['customerId']
            elif 'customer_id' in template_data:
                template.customer_id = template_data['customer_id']

            self.db.session.commit()
            
            self.logger.info(f"Template updated successfully: {template_id}")
            return self._format_template(template)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error updating template: {str(e)}")
            raise

    def delete_template(self, template_id):
        try:
            user_id = g.get('firebase_id')
            template = self.db.session.query(InvoiceTemplate).filter_by(
                id=template_id, 
                user_id=user_id
            ).first()

            if not template:
                raise ValueError("Template not found")

            self.db.session.delete(template)
            self.db.session.commit()
            
            self.logger.info(f"Template deleted successfully: {template_id}")
            return True

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error deleting template: {str(e)}")
            raise

    def update_customer_default_template(self, customer_id, template_data):
        try:
            user_id = g.get('firebase_id')
            
            # Verify customer exists and belongs to user
            customer = self.db.session.query(Customer).filter_by(
                id=customer_id, 
                user_id=user_id
            ).first()

            if not customer:
                raise ValueError("Customer not found")

            template_name = template_data.get('name', f"Default Template for {customer.name}")
            template_content = template_data.get('templateData') or template_data.get('template_data')
            
            # Check if a template with this name already exists for this customer
            existing_template = self.db.session.query(InvoiceTemplate).filter_by(
                customer_id=customer_id,
                user_id=user_id,
                name=template_name
            ).first()

            if existing_template:
                # Template exists - mark it as default and update its name
                existing_template.template_data = template_content
                existing_template.make_default_for_customer(self.db.session)
                template = existing_template
                self.logger.info(f"Existing template marked as default: {existing_template.id}")
            else:
                # Template doesn't exist - create new one and mark as default
                template = InvoiceTemplate(
                    user_id=user_id,
                    customer_id=customer_id,
                    name=template_name,
                    template_data=template_content,
                    is_customer_default=False  # Will be set by make_default_for_customer
                )
                
                self.db.session.add(template)
                self.db.session.flush()  # Get the ID
                template.make_default_for_customer(self.db.session)
                self.logger.info(f"New template created and marked as default: {template.id}")

            self.db.session.commit()
            
            self.logger.info(f"Customer default template updated: {customer_id}")
            return self._format_template(template)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error updating customer default template: {str(e)}")
            raise

    def _format_template(self, template):
        """Format template to match InvoiceTemplate interface"""
        formatted_template = {
            "id": template.id,
            "name": template.name,
            "templateData": template.template_data,  # Assumes this is already InvoiceData format
            "customerId": template.customer_id,
            "isCustomerDefault": template.is_customer_default,
            "createdAt": template.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if template.created_at else "",
            "updatedAt": template.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if template.updated_at else ""
        }
        return formatted_template