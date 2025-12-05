import streamlit as st
from pathlib import Path
from datetime import datetime
import pandas as pd
import tempfile

from src.database.db_utils import insert_raw_bill_document
from src.agents.document_processor_agent.utility_bill_doc_processor import process_bill
from src.utils.aws_app import (
    upload_fileobject_to_s3,
    get_s3_key,
    download_to_temp
)


def render_file_uploader():
    st.title("📁 File Upload Management")
    # Session flags to manage UI state
    if "bill_processed" not in st.session_state:
        st.session_state["bill_processed"] = False
    if "bill_results" not in st.session_state:
        st.session_state["bill_results"] = None

    # Tab navigation for separate sections
    tab1, tab2 = st.tabs(["📄 Bill Documents", "⚡ Tariff Documents"])

    # ====================================
    # TAB 1: Bill Upload
    # ====================================
    with tab1:
        st.subheader("📄 Bill Documents Management")
        
        st.markdown("### 📤 Upload New Bill")
        st.caption("Upload your utility bill (PDF only)")
        
        bill_file = st.file_uploader(
            "Choose a PDF bill file",
            type=["pdf"],
            accept_multiple_files=False,
            key="bill_uploader"
        )

        if bill_file:
            file = bill_file
            
            # Upload to S3
            s3_key = get_s3_key("raw", file.name)
            if not upload_fileobject_to_s3(file, s3_key):
                st.error(f"Failed to upload {file.name} to S3")
                st.stop()
            
            # Download to temp for processing
            temp_path = download_to_temp(s3_key)
            if not temp_path:
                st.error(f"Failed to download {file.name} from S3 for processing")
                st.stop()
            
            file_path = Path(temp_path)

            # Log upload in DB
            metadata = {
                "file_name": file.name,
                "file_type": Path(file.name).suffix.lower(),
                "upload_date": datetime.utcnow(),
                "source": "User Upload (Bill)",
                "status": "uploaded",
                "s3_key": s3_key
            }

            try:
                insert_raw_bill_document(metadata)
            except Exception as e:
                st.error(f"Error logging bill file {file.name}: {e}")

            # -------------------------
            # 🔥 AUTO-PROCESS THE FILE
            # -------------------------
            try:
                # Create a full-page modal overlay
                st.markdown("""
                    <style>
                    .stApp {
                        pointer-events: none;
                    }
                    div[data-testid="stAppViewContainer"] > section {
                        filter: blur(5px);
                    }
                    section[data-testid="stSidebar"] {
                        pointer-events: none;
                        filter: blur(5px);
                    }
                    </style>
                """, unsafe_allow_html=True)
                
                processing_placeholder = st.empty()
                
                with processing_placeholder.container():
                    st.markdown("""
                        <div style='position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; 
                             background: rgba(0, 0, 0, 0.7); backdrop-filter: blur(8px);
                             z-index: 9999; display: flex; align-items: center; justify-content: center;
                             pointer-events: all;'>
                            <div style='background: white; padding: 40px; border-radius: 10px; 
                                 text-align: center; box-shadow: 0 4px 20px rgba(0,0,0,0.3);'>
                                <h2 style='color: #1f77b4; margin-bottom: 20px;'>🔄 Processing Bill Document</h2>
                                <p style='font-size: 18px; font-weight: bold; color: #333; margin-bottom: 10px;'>{}</p>
                                <p style='color: #666; margin-bottom: 20px;'>Please wait while we extract and validate the billing data...</p>
                                <div style='width: 100%; height: 4px; background: #e0e0e0; border-radius: 2px; overflow: hidden;'>
                                    <div style='width: 50%; height: 100%; background: linear-gradient(90deg, #1f77b4, #4fc3f7); 
                                         animation: loading 1.5s ease-in-out infinite;'></div>
                                </div>
                            </div>
                        </div>
                        <style>
                        @keyframes loading {{
                            0% {{ transform: translateX(-100%); }}
                            50% {{ transform: translateX(100%); }}
                            100% {{ transform: translateX(-100%); }}
                        }}
                        </style>
                    """.format(file.name), unsafe_allow_html=True)
                
                # Process the file
                df, total_anomalies = process_bill(file_path)
                
                # Clean up temp file
                import os
                try:
                    os.unlink(temp_path)
                except:
                    pass
                
                # Clear the processing overlay and re-enable page
                processing_placeholder.empty()
                st.markdown("""
                    <style>
                    .stApp {
                        pointer-events: auto;
                    }
                    div[data-testid="stAppViewContainer"] > section {
                        filter: none;
                    }
                    section[data-testid="stSidebar"] {
                        pointer-events: auto;
                        filter: none;
                    }
                    </style>
                """, unsafe_allow_html=True)

                # Display results in a clean card layout
                st.markdown(f"### 📄 {file.name}")
                
                # Anomalies metric with tip on the right
                col1, col2 = st.columns([1, 3])
                with col1:
                    st.metric(label="Anomalies detected", value=int(total_anomalies))
                with col2:
                    st.info("💡 Tip: check Audit Bills section to get better insights.")
                
                # Data table with index starting from 1
                df_display = df.copy()
                df_display.index = df_display.index + 1
                st.dataframe(df_display, width='stretch')

                # Persist results and hide uploader on rerun
                st.session_state["bill_processed"] = True
                st.session_state["bill_results"] = {
                    "file_name": file.name,
                    "total_anomalies": int(total_anomalies),
                    "dataframe": df_display
                }
                # Clear file_uploader value and rerun to hide the chip
                if "bill_uploader" in st.session_state:
                    del st.session_state["bill_uploader"]
                st.rerun()

            except Exception as e:
                st.error(f"❌ Failed to process {file.name}: {e}")

        # When processed, show results from session
        if st.session_state["bill_processed"] and st.session_state["bill_results"]:
            res = st.session_state["bill_results"]
            st.markdown(f"### 📄 {res['file_name']}")
            col1, col2 = st.columns([1, 3])
            with col1:
                st.metric(label="Anomalies detected", value=res["total_anomalies"])
            with col2:
                st.info("💡 Tip: check Audit Bills section to get better insights.")
            st.dataframe(res["dataframe"], width='stretch')
            
            # Highlight the "Upload another bill" button with a more prominent color
            st.markdown(
                """
                <style>
                div[data-testid="stButton"] > button {
                    background-color: #ff9800 !important;
                    color: white !important;
                    border: none !important;
                    box-shadow: 0 2px 6px rgba(255, 152, 0, 0.4) !important;
                }
                div[data-testid="stButton"] > button:hover {
                    background-color: #fb8c00 !important;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )
            if st.button("Upload another bill"):
                st.session_state["bill_processed"] = False
                st.session_state["bill_results"] = None
                if "bill_uploader" in st.session_state:
                    del st.session_state["bill_uploader"]
                st.rerun()

    # ====================================
    # TAB 2: Tariff Upload
    # ====================================
    with tab2:
        st.subheader("Upload Tariff Documents")
        st.caption("Upload the latest tariff document for your utility provider (PDF only).")

        # store results
        if "tariff_results" not in st.session_state:
            st.session_state["tariff_results"] = []

        tariff_files = st.file_uploader(
            "Choose tariff PDF files",
            type=["pdf"],
            accept_multiple_files=True,
            key="tariff_uploader"
        )

        # If already processed -> show results cleanly
        if st.session_state["tariff_results"]:
            st.markdown("### 📦 Processed Tariff Files")

            for result in st.session_state["tariff_results"]:
                st.success(f"✔ {result['name']}")
                st.json({
                    "Grouped Tariffs": str(result["grouped"]),
                    "Final Logic": str(result["logic"])
                })

            if st.button("Upload More Tariff Files"):
                st.session_state["tariff_results"] = []
                st.session_state["tariff_uploader"] = None
                st.rerun()

        # If uploading new files -> process them
        elif tariff_files:
            for file in tariff_files:
                try:
                    # ---------- FULL SCREEN OVERLAY ----------
                    overlay = st.empty()
                    overlay.markdown(f"""
                        <style>
                            .stApp {{ pointer-events:none; }}
                            div[data-testid="stAppViewContainer"] > section {{ filter:blur(5px); }}
                            section[data-testid="stSidebar"] {{ filter:blur(5px); pointer-events:none; }}
                        </style>

                        <div style='position:fixed; top:0; left:0; width:100vw; height:100vh;
                                    background:rgba(0,0,0,0.7); z-index:9999;
                                    display:flex; justify-content:center; align-items:center;'>
                            <div style='background:white; padding:40px; border-radius:12px;
                                        width:450px; text-align:center;'>
                                <h2 style='color:#1f77b4;'>🔄 Processing Tariff</h2>
                                <p style='font-size:18px; font-weight:600;'>{file.name}</p>
                                <p style='color:#666;'>Extracting, grouping and analyzing tariff...</p>

                                <div style='width:100%; height:6px; background:#e0e0e0; border-radius:4px; overflow:hidden;'>
                                    <div style='width:50%; height:100%;
                                        background:linear-gradient(90deg,#1f77b4,#4fc3f7);
                                        animation:loading 1.5s ease-in-out infinite;'></div>
                                </div>
                            </div>
                        </div>

                        <style>
                        @keyframes loading {{
                            0% {{ transform:translateX(-100%); }}
                            50% {{ transform:translateX(100%); }}
                            100% {{ transform:translateX(-100%); }}
                        }}
                        </style>
                    """, unsafe_allow_html=True)

                    # ---------- UPLOAD TO S3 ----------
                    s3_key = get_s3_key("raw/tariff", file.name)
                    if not upload_fileobject_to_s3(file, s3_key):
                        raise Exception(f"Failed to upload {file.name} to S3")
                    
                    # ---------- DOWNLOAD TO TEMP FOR PROCESSING ----------
                    temp_path = download_to_temp(s3_key)
                    if not temp_path:
                        raise Exception(f"Failed to download {file.name} from S3")
                    
                    file_path = Path(temp_path)

                    # ---------- RUN PIPELINE ----------
                    from src.agents.tariff_analysis_agent.pipeline_runner import run_tariff_pipeline
                    results = run_tariff_pipeline(file_path)
                    
                    # Clean up temp file
                    import os
                    try:
                        os.unlink(temp_path)
                    except:
                        pass

                    # ---------- SAVE RESULTS ----------
                    st.session_state["tariff_results"].append({
                        "name": file.name,
                        "grouped": results["grouped_tariffs"],
                        "logic": results["final_logic"]
                    })

                    # ---------- CLEAR OVERLAY + REFRESH ----------
                    overlay.empty()
                    st.session_state["tariff_uploader"] = None
                    st.rerun()

                except Exception as e:
                    st.error(f"Error processing {file.name}: {e}")
                    st.info("Please try uploading the file again.")
