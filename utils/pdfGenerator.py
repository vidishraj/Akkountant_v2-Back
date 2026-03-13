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

    @staticmethod
    def _get(data, camel_key, snake_key, default=None):
        """Get a value trying camelCase first, then snake_case."""
        return data.get(camel_key, data.get(snake_key, default))

    def generate_invoice_pdf(self, invoice_data):
        try:
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72,
                                  topMargin=72, bottomMargin=18)

            story = []
            styles = getSampleStyleSheet()

            # Get currency symbol — handle enum values
            currency = invoice_data.get('currency', 'USD')
            if hasattr(currency, 'value'):
                currency = currency.value
            currency_symbol = self.currency_symbols.get(str(currency), '$')
            
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
                ['Invoice Number:', self._get(invoice_data, 'invoiceNumber', 'invoice_number', 'N/A')],
                ['Project:', self._get(invoice_data, 'projectName', 'project_name', 'N/A')],
                ['Issue Date:', str(self._get(invoice_data, 'issueDate', 'issue_date', 'N/A'))],
                ['Due Date:', str(self._get(invoice_data, 'dueDate', 'due_date', 'N/A'))]
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
            
            # From and To section — support both flat snake_case and nested camelCase
            from_data = invoice_data.get('from', {})
            to_data = invoice_data.get('to', {})
            from_name = from_data.get('name') or invoice_data.get('from_name', '')
            from_email = from_data.get('email') or invoice_data.get('from_email', '')
            from_address = from_data.get('address') or invoice_data.get('from_address', '')
            from_phone = from_data.get('phone') or invoice_data.get('from_phone', '')
            to_name = to_data.get('name') or invoice_data.get('to_name', '')
            to_email = to_data.get('email') or invoice_data.get('to_email', '')
            to_address = to_data.get('address') or invoice_data.get('to_address', '')
            to_company = to_data.get('company') or invoice_data.get('to_company', '')

            from_to_data = [
                ['FROM:', 'TO:'],
                [f"{from_name}", f"{to_name}"],
                [f"{from_email}", f"{to_email}"],
                [f"{from_address}", f"{to_address}"]
            ]

            if from_phone:
                from_to_data.append([f"Phone: {from_phone}", ''])
            if to_company:
                from_to_data[1][1] = f"{to_company}\n{from_to_data[1][1]}"
            
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
        """Overlay a base64-encoded signature image onto the existing PDF."""
        try:
            from PyPDF2 import PdfReader, PdfWriter
            from reportlab.lib.units import mm

            if not signature_data:
                self.logger.warning("No signature data provided, returning original PDF")
                return pdf_data

            # Decode the signature image
            sig_bytes = signature_data
            if isinstance(sig_bytes, str):
                # Strip data URL prefix if present
                if ',' in sig_bytes:
                    sig_bytes = sig_bytes.split(',', 1)[1]
                sig_bytes = base64.b64decode(sig_bytes)

            # Create a PDF page with just the signature using reportlab
            sig_buffer = io.BytesIO()
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas as rl_canvas

            c = rl_canvas.Canvas(sig_buffer, pagesize=letter)
            sig_image = io.BytesIO(sig_bytes)

            x = position.get('x', 100) * mm
            # reportlab y=0 is bottom; convert from top-origin
            page_height = letter[1]
            y = page_height - position.get('y', 100) * mm - position.get('height', 50) * mm
            w = position.get('width', 100) * mm
            h = position.get('height', 50) * mm

            try:
                from reportlab.lib.utils import ImageReader
                img = ImageReader(sig_image)
                c.drawImage(img, x, y, width=w, height=h, mask='auto')
            except Exception as img_err:
                self.logger.warning(f"Could not draw signature image: {img_err}")
                return pdf_data

            c.save()
            sig_buffer.seek(0)

            # Merge the signature overlay onto every page of the original PDF
            original_reader = PdfReader(io.BytesIO(pdf_data))
            sig_reader = PdfReader(sig_buffer)
            writer = PdfWriter()

            sig_page = sig_reader.pages[0]
            for page in original_reader.pages:
                page.merge_page(sig_page)
                writer.add_page(page)

            output = io.BytesIO()
            writer.write(output)
            output.seek(0)

            self.logger.info("Signature added to PDF successfully")
            return output.getvalue()

        except Exception as e:
            self.logger.error(f"Error adding signature to PDF: {str(e)}")
            raise