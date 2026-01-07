let currentJobId = null;

// Initialize dashboard functionality
document.addEventListener('DOMContentLoaded', function() {
  const fileUploadArea = document.getElementById('fileUploadArea');
  const fileInput = document.getElementById('files');
  const uploadBtn = document.getElementById('uploadBtn');
  
  // File input change handler
  fileInput.addEventListener('change', handleFileSelection);
  
  // Drag and drop handlers
  fileUploadArea.addEventListener('click', () => fileInput.click());
  fileUploadArea.addEventListener('dragover', handleDragOver);
  fileUploadArea.addEventListener('dragleave', handleDragLeave);
  fileUploadArea.addEventListener('drop', handleDrop);
  
  function handleDragOver(e) {
    e.preventDefault();
    fileUploadArea.classList.add('dragover');
  }
  
  function handleDragLeave(e) {
    e.preventDefault();
    fileUploadArea.classList.remove('dragover');
  }
  
  function handleDrop(e) {
    e.preventDefault();
    fileUploadArea.classList.remove('dragover');
    
    const files = e.dataTransfer.files;
    fileInput.files = files;
    handleFileSelection();
  }
  
  function handleFileSelection() {
    const files = fileInput.files;
    const fileList = document.getElementById('fileList');
    
    if (files.length > 0) {
      displayFileList(files);
      fileList.classList.remove('hidden');
      uploadBtn.disabled = false;
      updateHeaderStats(`${files.length} files selected`);
    } else {
      fileList.classList.add('hidden');
      uploadBtn.disabled = true;
      updateHeaderStats('Ready');
    }
  }
  
  function displayFileList(files) {
    const fileList = document.getElementById('fileList');
    let html = '<h4><i class="fas fa-file-alt"></i> Selected Files</h4>';
    
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const fileSize = (file.size / 1024 / 1024).toFixed(2);
      const fileIcon = file.name.endsWith('.xlsx') ? 'fa-file-excel' : 'fa-file-csv';
      
      html += `
        <div class="file-item">
          <i class="fas ${fileIcon}"></i>
          <span>${file.name}</span>
          <small>${fileSize} MB</small>
        </div>
      `;
    }
    
    fileList.innerHTML = html;
  }
});

function updateHeaderStats(status) {
  const headerStats = document.getElementById('headerStats');
  headerStats.innerHTML = `
    <div class="stat-item">
      <i class="fas fa-database"></i>
      <span>${status}</span>
    </div>
  `;
}

function upload() {
  const files = document.getElementById("files").files;
  if (files.length === 0) {
    showError("Please select files first.");
    return;
  }

  const data = new FormData();
  for (let i = 0; i < files.length; i++) {
    data.append("files", files[i]);
  }

  showStatus("Uploading files and running pipeline...", "loading");
  updateHeaderStats("Processing pipeline...");
  disableUI(true);

  fetch("/upload", { method: "POST", body: data })
    .then(r => {
      if (!r.ok) {
        return r.json().then(err => {
          throw new Error(err.error || "Pipeline failed");
        });
      }
      return r.json();
    })
    .then(d => {
      currentJobId = d.job_id;
      showStatus("Pipeline completed successfully!", "success");
      updateHeaderStats("Pipeline completed");
      
      // Show results and enable search
      const resultsSection = document.getElementById("resultsSection");
      const searchSection = document.getElementById("searchSection");
      const searchPlaceholder = document.getElementById("searchPlaceholder");
      
      resultsSection.classList.remove("hidden");
      searchSection.classList.remove("hidden");
      searchPlaceholder.classList.add("hidden");

      document.getElementById("result").innerHTML = `
        <i class="fas fa-check-circle"></i>
        <div>
          <h3>Processing Complete!</h3>
          <p>Your data has been successfully processed through the pipeline.</p>
          <a href="${d.download}">
            <i class="fas fa-download"></i>
            Download final_data.csv
          </a>
        </div>
      `;
    })
    .catch(err => {
      showStatus(`Error: ${err.message}`, "error");
      updateHeaderStats("Pipeline failed");
      console.error("Upload error:", err);
    })
    .finally(() => {
      disableUI(false);
    });
}

function searchLot() {
  if (!currentJobId) {
    showError("Please run the pipeline first.");
    return;
  }

  const lotA = document.getElementById("lotA").value.trim();
  const lotB = document.getElementById("lotB").value.trim();

  if (!lotA && !lotB) {
    showError("Please enter at least one LOT (A or B) to search.");
    return;
  }

  const data = new FormData();
  data.append("job_id", currentJobId);
  data.append("lot_a", lotA);
  data.append("lot_b", lotB);

  showStatus("Searching LOT trace...", "loading");
  updateHeaderStats("Searching LOT data...");
  disableUI(true);

  fetch("/search", { method: "POST", body: data })
    .then(response => {
      if (!response.ok) {
        return response.json().then(err => {
          throw new Error(err.error || "Search failed");
        });
      }
      return response.blob();
    })
    .then(blob => {
      // Create download link
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "lot_trace_result.csv";
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);

      showStatus("LOT trace exported successfully!", "success");
      updateHeaderStats("Export completed");
    })
    .catch(err => {
      showStatus(`Search Error: ${err.message}`, "error");
      updateHeaderStats("Search failed");
      console.error("Search error:", err);
    })
    .finally(() => {
      disableUI(false);
    });
}

function showStatus(message, type = "loading") {
  const status = document.getElementById("status");
  const statusText = document.getElementById("statusText");
  
  status.className = `status-panel ${type}`;
  statusText.textContent = message;
  status.classList.remove("hidden");
  
  // Auto-hide success/error messages after 5 seconds
  if (type === "success" || type === "error") {
    setTimeout(() => {
      status.classList.add("hidden");
    }, 5000);
  }
}

function showError(message) {
  showStatus(message, "error");
}

function disableUI(disabled) {
  document.querySelectorAll("button, input").forEach(el => {
    el.disabled = disabled;
    el.style.opacity = disabled ? "0.6" : "1";
  });
}

// Help panel functionality
function toggleHelp() {
  const helpPanel = document.getElementById('helpPanel');
  helpPanel.classList.toggle('open');
}

// Close help panel when clicking outside
document.addEventListener('click', function(e) {
  const helpPanel = document.getElementById('helpPanel');
  const helpToggle = document.querySelector('.help-toggle');
  
  if (helpPanel.classList.contains('open') && 
      !helpPanel.contains(e.target) && 
      !helpToggle.contains(e.target)) {
    helpPanel.classList.remove('open');
  }
});

// Keyboard shortcuts
document.addEventListener('keydown', function(e) {
  // ESC to close help
  if (e.key === 'Escape') {
    const helpPanel = document.getElementById('helpPanel');
    helpPanel.classList.remove('open');
  }
  
  // F1 to open help
  if (e.key === 'F1') {
    e.preventDefault();
    toggleHelp();
  }
});

