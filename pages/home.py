import dash
from dash import html

dash.register_page(__name__, path='/')

layout = html.Div(
    [
        html.H1(
            'Welcome!', 
            style={'text-align': 'center', 'margin-bottom': '10px'}  # Reduced bottom margin
        ),
        html.Div(
            'Select the dashboard you want to view from the options above.',
            style={'text-align': 'center', 'margin-bottom': '20px'}  # Reduced bottom margin
        ),
        html.Div(
            html.Img(src='assets/aml image.webp', style={'width': '500px', 'height': '500px'}),
            style={
                'display': 'flex',
                'justify-content': 'center',
                'align-items': 'center',
                'height': '100vh',
                'flex-direction': 'column'  # Aligns items vertically
            }
        )
    ]
)