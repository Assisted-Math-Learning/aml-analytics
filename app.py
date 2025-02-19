import dash
from dash import Dash, html, dcc

app = Dash(__name__, use_pages=True, suppress_callback_exceptions=True)
server = app.server


def layout():
    return html.Div(
        [
            # Light Grey Ribbon for Header Section
            html.Div(
                [
                    # Monitoring Dashboards Heading
                    html.Div(
                        [
                            html.H1(
                                "Monitoring Dashboards",
                                style={
                                    "font-family": "Arial, sans-serif",
                                    "font-size": "20px",  # Subtle size for the header
                                    "margin": "0",
                                    "padding-right": "10px",
                                    "color": "#333",  # Dark gray for the heading
                                    "display": "inline",  # Keep it inline with the arrow and buttons
                                },
                            ),
                            html.Span(
                                "➔",
                                style={
                                    "font-size": "16px",  # Smaller arrow
                                    "color": "#333",  # Matches the text color
                                    "margin-right": "10px",
                                    "display": "inline",  # Inline with the heading
                                },
                            ),
                        ],
                        style={
                            "text-align": "center",  # Center the heading and arrow
                            "margin-bottom": "20px",  # Space below the heading
                        },
                    ),
                    html.Div(
                        id="digital-data-dash-links",
                        children=[
                            dcc.Link(
                                f"{page['name']}",
                                href=page["relative_path"],
                                style={
                                    "display": "inline-block",
                                    "padding": "5px 15px",  # Compact button size
                                    "margin-right": "8px",  # Tight spacing between buttons
                                    "text-decoration": "none",
                                    "color": "#FFF",  # White text
                                    "background-color": "#5A9BD5",  # Button blue color
                                    "border-radius": "4px",
                                    "font-size": "14px",
                                    "font-family": "Arial, sans-serif",
                                    "text-align": "center",
                                    "box-shadow": "0 1px 3px rgba(0, 0, 0, 0.1)",
                                },
                            )
                            for page in dash.page_registry.values()
                            if page["name"]
                            in [
                                "Digital master dashboard",
                                "Digital learners progress dashboard",
                                "Digital grade performance dashboard",
                                "Digital qset performance dashboard",
                                "Digital question performance dashboard",
                            ]
                        ],
                    ),
                ],
                style={
                    "background-color": "#F5F5F5",  # Lighter grey background for the ribbon
                    "width": "100%",  # Full width
                    "text-align": "center",  # Keep the text and buttons aligned left
                    "padding": "10px 20px",  # Padding for spacing
                },
            ),
            # Main content section (full-width)
            html.Div(
                dash.page_container,
                style={
                    "padding": "20px",  # Adds breathing space for content
                    "background-color": "#FFF",  # Consistent white background across the page
                    "margin": "0",
                    "min-height": "100vh",  # Ensures the page takes up full height
                },
            ),
        ],
        style={
            "font-family": "Roboto, sans-serif",
            "margin": "0",
            "padding": "0",
            "box-sizing": "border-box",
            "background-color": "#FFF",  # Entire page background is white
        },
    )


app.layout = layout

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", debug=True)
