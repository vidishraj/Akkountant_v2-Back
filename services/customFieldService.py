from flask import g
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from models.freelance_management import Invoice, InvoiceCustomField
from services.Base_Service import BaseService
from utils.logger import Logger


class CustomFieldService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CustomFieldService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()

    def get_custom_fields(self, invoice_id):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Verify invoice belongs to user
            invoice = self.db.session.query(Invoice).filter_by(
                id=invoice_id, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            custom_fields = self.db.session.query(InvoiceCustomField).filter_by(
                invoice_id=invoice_id
            ).order_by(InvoiceCustomField.sort_order).all()

            return [self._format_custom_field(field) for field in custom_fields]

        except Exception as e:
            self.logger.error(f"Error fetching custom fields: {str(e)}")
            raise

    def add_custom_field(self, invoice_id, field_data):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Verify invoice belongs to user
            invoice = self.db.session.query(Invoice).filter_by(
                id=invoice_id, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            custom_field = InvoiceCustomField(
                invoice_id=invoice_id,
                field_key=field_data['fieldKey'],
                field_value=field_data['fieldValue'],
                is_hidden=field_data.get('isHidden', False),
                sort_order=field_data.get('sortOrder', 0)
            )

            self.db.session.add(custom_field)
            self.db.session.commit()
            
            self.logger.info(f"Custom field added successfully: {custom_field.id}")
            return self._format_custom_field(custom_field)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error adding custom field: {str(e)}")
            raise

    def update_custom_field(self, invoice_id, field_id, field_data):
        try:
            user_id = g.get('firebase_id')
            
            # Verify invoice belongs to user
            invoice = self.db.session.query(Invoice).filter_by(
                id=invoice_id, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            custom_field = self.db.session.query(InvoiceCustomField).filter_by(
                id=field_id,
                invoice_id=invoice_id
            ).first()

            if not custom_field:
                raise ValueError("Custom field not found")

            # Update fields
            if 'fieldKey' in field_data:
                custom_field.field_key = field_data['fieldKey']
            if 'fieldValue' in field_data:
                custom_field.field_value = field_data['fieldValue']
            if 'isHidden' in field_data:
                custom_field.is_hidden = field_data['isHidden']
            if 'sortOrder' in field_data:
                custom_field.sort_order = field_data['sortOrder']

            self.db.session.commit()
            
            self.logger.info(f"Custom field updated successfully: {field_id}")
            return self._format_custom_field(custom_field)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error updating custom field: {str(e)}")
            raise

    def delete_custom_field(self, invoice_id, field_id):
        try:
            user_id = g.get('firebase_id')
            
            # Verify invoice belongs to user
            invoice = self.db.session.query(Invoice).filter_by(
                id=invoice_id, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            custom_field = self.db.session.query(InvoiceCustomField).filter_by(
                id=field_id,
                invoice_id=invoice_id
            ).first()

            if not custom_field:
                raise ValueError("Custom field not found")

            self.db.session.delete(custom_field)
            self.db.session.commit()
            
            self.logger.info(f"Custom field deleted successfully: {field_id}")
            return True

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error deleting custom field: {str(e)}")
            raise

    def _format_custom_field(self, field):
        """Format custom field data for API response"""
        return {
            "id": field.id,
            "fieldKey": field.field_key,
            "fieldValue": field.field_value,
            "isHidden": field.is_hidden,
            "sortOrder": field.sort_order,
            "createdAt": field.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if field.created_at else ""
        }
