(() => {
  const form = document.querySelector("[data-product-form]");
  if (!form) return;

  const fileInput = form.querySelector("#product-photos");
  const sessionInput = form.querySelector("#upload-session-id");
  const trigger = form.querySelector("[data-photo-trigger]");
  const clearButton = form.querySelector("[data-photo-clear]");
  const strip = form.querySelector("[data-photo-strip]");
  const statusEl = form.querySelector("[data-analysis-status]");
  if (!fileInput || !sessionInput || !strip || !statusEl) return;

  const pickupNumber = form.dataset.pickup;
  const palletNumber = form.dataset.pallet;
  if (!pickupNumber || !palletNumber) return;

  const MAX_FILES = 10;
  const MAX_TOTAL_BYTES = 25 * 1024 * 1024;
  const defaultStatus = statusEl.textContent.trim();

  const selectedFiles = [];
  let analysisController = null;
  let analyzing = false;

  const generateId = () => {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      return crypto.randomUUID();
    }
    return `${Date.now().toString(16)}-${Math.random().toString(16).slice(2, 8)}`;
  };

  const revokeAllPreviews = () => {
    selectedFiles.forEach((item) => {
      if (item.previewUrl) {
        URL.revokeObjectURL(item.previewUrl);
      }
    });
  };

  const showStatus = (message, tone = "") => {
    const text = message && message.trim().length ? message : defaultStatus;
    statusEl.textContent = text;
    if (tone) {
      statusEl.dataset.tone = tone;
      return;
    }
    delete statusEl.dataset.tone;
  };

  const updateClearButton = () => {
    if (!clearButton) return;
    clearButton.hidden = selectedFiles.length === 0;
  };

  const syncFileInput = () => {
    if (!window.DataTransfer) return;
    const dt = new DataTransfer();
    selectedFiles.forEach(({ file }) => dt.items.add(file));
    fileInput.files = dt.files;
  };

  const clearSession = () => {
    sessionInput.value = "";
  };

  const renderStrip = () => {
    strip.innerHTML = "";
    if (!selectedFiles.length) return;

    selectedFiles.forEach((item, index) => {
      const thumb = document.createElement("div");
      thumb.className = "photo-thumb";

      const image = document.createElement("img");
      image.src = item.previewUrl;
      image.alt = `Product photo ${index + 1}`;
      thumb.appendChild(image);

      const removeBtn = document.createElement("button");
      removeBtn.type = "button";
      removeBtn.textContent = "Remove";
      removeBtn.addEventListener("click", () => {
        removeFile(item.id);
      });
      thumb.appendChild(removeBtn);

      strip.appendChild(thumb);
    });
  };

  const totalBytes = () => selectedFiles.reduce((sum, entry) => sum + entry.file.size, 0);

  const removeFile = (id) => {
    const index = selectedFiles.findIndex((entry) => entry.id === id);
    if (index === -1) return;
    const [removed] = selectedFiles.splice(index, 1);
    if (removed && removed.previewUrl) {
      URL.revokeObjectURL(removed.previewUrl);
    }
    clearSession();
    syncFileInput();
    renderStrip();
    updateClearButton();
    if (selectedFiles.length) {
      queueAnalysis();
    } else {
      if (analysisTimeout) {
        window.clearTimeout(analysisTimeout);
        analysisTimeout = null;
      }
      showStatus(defaultStatus);
    }
  };

  const handleFiles = (fileList) => {
    const incoming = Array.from(fileList || []).filter(Boolean);
    if (!incoming.length) return;

    const newCount = selectedFiles.length + incoming.length;
    if (newCount > MAX_FILES) {
      showStatus(`Limit ${MAX_FILES} photos per product. Remove some before adding more.`, "error");
      return;
    }

    const incomingBytes = incoming.reduce((sum, file) => sum + file.size, 0);
    if (totalBytes() + incomingBytes > MAX_TOTAL_BYTES) {
      showStatus("Images exceed the 25 MB limit. Remove large files before retrying.", "error");
      return;
    }

    incoming.forEach((file) => {
      selectedFiles.push({
        id: generateId(),
        file,
        previewUrl: URL.createObjectURL(file),
      });
    });

    clearSession();
    syncFileInput();
    renderStrip();
    updateClearButton();
    queueAnalysis();
  };

  const gentlySetValue = (element, value) => {
    if (!element || !value) return;
    if (element.tagName === "SELECT") {
      const stringValue = String(value);
      const option = Array.from(element.options).find((opt) => opt.value === stringValue);
      if (option) {
        element.value = stringValue;
      }
      return;
    }
    if (element.value.trim().length === 0) {
      element.value = value;
    }
  };

  const applySuggestions = (suggestions = {}) => {
    const serialField = form.querySelector("#sn");
    const assetTagField = form.querySelector("#asset_tag");
    const descriptionField = form.querySelector("#description");
    const subcategoryField = form.querySelector("#subcategory");
    const destinationField = form.querySelector("#cod_destiny");

    gentlySetValue(serialField, suggestions.serial_number);
    gentlySetValue(assetTagField, suggestions.asset_tag);

    if (descriptionField && suggestions.short_description) {
      if (descriptionField.value.trim().length === 0) {
        descriptionField.value = suggestions.short_description;
      }
    }

    gentlySetValue(subcategoryField, suggestions.subcategory);

    if (destinationField && suggestions.cod_destiny !== null && suggestions.cod_destiny !== undefined) {
      gentlySetValue(destinationField, suggestions.cod_destiny);
    }
  };

  const runAnalysis = async () => {
    if (!selectedFiles.length) {
      return;
    }
    if (analysisTimeout) {
      window.clearTimeout(analysisTimeout);
      analysisTimeout = null;
    }
    if (analysisController) {
      analysisController.abort();
    }
    analysisController = new AbortController();
    analyzing = true;
    showStatus("Analyzing photos…", "progress");

    const formData = new FormData();
    selectedFiles.forEach(({ file }) => formData.append("photos", file));

    try {
      const response = await fetch(
        `/pickups/${pickupNumber}/pallets/${palletNumber}/products/analyze`,
        {
          method: "POST",
          body: formData,
          credentials: "same-origin",
          signal: analysisController.signal,
        },
      );
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.status !== "ok") {
        const message = payload.message || "Photo analysis failed. Please submit manually or retry.";
        clearSession();
        showStatus(message, "error");
        return;
      }
      sessionInput.value = payload.session || "";
      applySuggestions(payload.suggestions || {});
      showStatus("Suggestions ready—review and adjust if needed.", "success");
    } catch (error) {
      if (error.name === "AbortError") {
        return;
      }
      console.error("Photo analysis error", error);
      clearSession();
      showStatus("Network error while analyzing photos. Please retry.", "error");
    } finally {
      analyzing = false;
      analysisController = null;
    }
  };

  let analysisTimeout = null;
  const queueAnalysis = () => {
    if (analysisTimeout) {
      window.clearTimeout(analysisTimeout);
    }
    analysisTimeout = window.setTimeout(runAnalysis, 350);
  };

  trigger?.addEventListener("click", (event) => {
    event.preventDefault();
    if (analyzing) return;
    fileInput.click();
  });

  fileInput.addEventListener("change", (event) => {
    handleFiles(event.target.files);
  });

  clearButton?.addEventListener("click", () => {
    if (analysisTimeout) {
      window.clearTimeout(analysisTimeout);
      analysisTimeout = null;
    }
    revokeAllPreviews();
    selectedFiles.length = 0;
    clearSession();
    syncFileInput();
    renderStrip();
    updateClearButton();
    showStatus("Photos cleared. Add new photos to try again.", "info");
  });

  form.addEventListener("submit", () => {
    if (sessionInput.value) {
      if (window.DataTransfer) {
        const emptyTransfer = new DataTransfer();
        fileInput.files = emptyTransfer.files;
      } else {
        fileInput.value = "";
      }
    } else {
      syncFileInput();
    }
  });

  form.addEventListener("reset", () => {
    if (analysisTimeout) {
      window.clearTimeout(analysisTimeout);
      analysisTimeout = null;
    }
    revokeAllPreviews();
    selectedFiles.length = 0;
    clearSession();
    showStatus(defaultStatus);
    updateClearButton();
    strip.innerHTML = "";
  });

  updateClearButton();
})();
