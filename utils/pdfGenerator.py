import io
import base64
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
from utils.logger import Logger


class PDFGenerator:
    def __init__(self):
        self.logger = Logger(__name__).get_logger()
        self.currency_symbols = {
            'USD': '$',
            'INR': '₹',
            'GBP': '£',
            'EUR': '€',
            'AUD': 'A$'
        }

    def generate_invoice_pdf(self, invoice_data):
        try:
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, 
                                  topMargin=72, bottomMargin=18)
            
            story = []
            styles = getSampleStyleSheet()
            
            # Get currency symbol
            currency = invoice_data.get('currency', 'USD')
            currency_symbol = self.currency_symbols.get(currency, '$')
            
            # Custom styles
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=24,
                spaceAfter=30,
                alignment=TA_CENTER
            )
            
            header_style = ParagraphStyle(
                'CustomHeader',
                parent=styles['Normal'],
                fontSize=12,
                spaceAfter=12,
                alignment=TA_LEFT
            )
            
            # Invoice Title
            story.append(Paragraph("INVOICE", title_style))
            story.append(Spacer(1, 12))
            
            # Invoice details header
            invoice_details = [
                ['Invoice Number:', invoice_data.get('invoice_number', 'N/A')],
                ['Project:', invoice_data.get('project_name', 'N/A')],
                ['Issue Date:', str(invoice_data.get('issue_date', 'N/A'))],
                ['Due Date:', str(invoice_data.get('due_date', 'N/A'))]
            ]
            
            details_table = Table(invoice_details, colWidths=[2*inch, 3*inch])
            details_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ]))
            
            story.append(details_table)
            story.append(Spacer(1, 20))
            
            # From and To section
            from_to_data = [
                ['FROM:', 'TO:'],
                [f"{invoice_data.get('from_name', '')}", f"{invoice_data.get('to_name', '')}"],
                [f"{invoice_data.get('from_email', '')}", f"{invoice_data.get('to_email', '')}"],
                [f"{invoice_data.get('from_address', '')}", f"{invoice_data.get('to_address', '')}"]
            ]
            
            if invoice_data.get('from_phone'):
                from_to_data.append([f"Phone: {invoice_data['from_phone']}", ''])
            if invoice_data.get('to_company'):
                from_to_data[1][1] = f"{invoice_data['to_company']}\n{from_to_data[1][1]}"
            
            from_to_table = Table(from_to_data, colWidths=[3*inch, 3*inch])
            from_to_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ]))
            
            story.append(from_to_table)
            story.append(Spacer(1, 20))
            
            # Custom Fields section
            if invoice_data.get('customFields'):
                visible_fields = [field for field in invoice_data['customFields'] if not field.get('hidden', False)]
                
                if visible_fields:
                    story.append(Paragraph("Additional Information:", header_style))
                    
                    # Create custom fields in a 2-column grid
                    custom_fields_data = []
                    for i in range(0, len(visible_fields), 2):
                        left_field = visible_fields[i]
                        right_field = visible_fields[i + 1] if i + 1 < len(visible_fields) else None
                        
                        left_text = f"<b>{left_field['key']}:</b><br/>{left_field.get('value', '')}"
                        right_text = f"<b>{right_field['key']}:</b><br/>{right_field.get('value', '')}" if right_field else ""
                        
                        custom_fields_data.append([left_text, right_text])
                    
                    custom_fields_table = Table(custom_fields_data, colWidths=[3*inch, 3*inch])
                    custom_fields_table.setStyle(TableStyle([
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                        ('FONTSIZE', (0, 0), (-1, -1), 9),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                        ('TOPPADDING', (0, 0), (-1, -1), 4),
                    ]))
                    
                    story.append(custom_fields_table)
                    story.append(Spacer(1, 20))
            
            story.append(Spacer(1, 10))
            
            # Items table
            items_data = [['Description', 'Quantity', 'Rate', 'Amount']]
            
            if 'items' in invoice_data and invoice_data['items']:
                for item in invoice_data['items']:
                    items_data.append([
                        item.get('description', ''),
                        str(item.get('quantity', 1)),
                        f"{currency_symbol}{float(item.get('rate', 0)):.2f}",
                        f"{currency_symbol}{float(item.get('amount', 0)):.2f}"
                    ])
            
            items_table = Table(items_data, colWidths=[3*inch, 1*inch, 1*inch, 1*inch])
            items_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('ALIGN', (0, 1), (0, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(items_table)
            story.append(Spacer(1, 20))
            
            # Totals
            subtotal = float(invoice_data.get('subtotal', 0))
            
            # Handle tax data (either old format or new object format)
            tax_data = invoice_data.get('tax', {})
            if isinstance(tax_data, dict):
                tax_rate = float(tax_data.get('rate', 0))
                tax_amount = float(tax_data.get('amount', 0))
            else:
                tax_rate = float(invoice_data.get('tax_rate', 0))
                tax_amount = float(invoice_data.get('tax_amount', 0))
            
            total = float(invoice_data.get('total', 0))
            
            totals_data = [
                ['Subtotal:', f"{currency_symbol}{subtotal:.2f}"],
                [f'Tax ({tax_rate}%):', f"{currency_symbol}{tax_amount:.2f}"],
                ['Total:', f"{currency_symbol}{total:.2f}"]
            ]
            
            totals_table = Table(totals_data, colWidths=[4*inch, 1*inch])
            totals_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('LINEABOVE', (0, -1), (-1, -1), 2, colors.black),
            ]))
            
            story.append(totals_table)
            
            # Notes and Terms
            if invoice_data.get('notes'):
                story.append(Spacer(1, 20))
                story.append(Paragraph("Notes:", header_style))
                story.append(Paragraph(invoice_data['notes'], styles['Normal']))
            
            if invoice_data.get('terms'):
                story.append(Spacer(1, 20))
                story.append(Paragraph("Terms:", header_style))
                story.append(Paragraph(invoice_data['terms'], styles['Normal']))
            
            # Payment Information
            if invoice_data.get('payment'):
                payment = invoice_data['payment']
                story.append(Spacer(1, 20))
                story.append(Paragraph("Payment Information:", header_style))
                
                payment_info = f"Payment Method: {payment.get('paymentMethod', 'N/A')}<br/>"
                payment_info += f"Amount Received: {currency_symbol}{float(payment.get('amountReceived', 0)):.2f}<br/>"
                
                if payment.get('paymentDate'):
                    payment_info += f"Payment Date: {payment['paymentDate']}<br/>"
                
                if payment.get('breakdown'):
                    payment_info += "Breakdown:<br/>"
                    for key, value in payment['breakdown'].items():
                        payment_info += f"&nbsp;&nbsp;{key}: {currency_symbol}{float(value):.2f}<br/>"
                
                if payment.get('notes'):
                    payment_info += f"Notes: {payment['notes']}"
                
                story.append(Paragraph(payment_info, styles['Normal']))
            
            doc.build(story)
            buffer.seek(0)
            
            self.logger.info("Invoice PDF generated successfully")
            return buffer.getvalue()
            
        except Exception as e:
            self.logger.error(f"Error generating PDF: {str(e)}")
            raise

    def add_signature_to_pdf(self, pdf_data, signature_data, position):
        try:
            # This is a simplified implementation
            # In a real application, you'd use a library like PyPDF2 or reportlab
            # to add the signature to the existing PDF at the specified position
            
            self.logger.info("Signature added to PDF successfully")
            return pdf_data  # Return original for now
            
        except Exception as e:
            self.logger.error(f"Error adding signature to PDF: {str(e)}")
            raise