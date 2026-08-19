#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
mod config;
mod send;
mod receive;
use eframe::egui;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

#[derive(PartialEq, Clone, Copy)]
enum Action {
    Send,
    Receive,
}

struct NymXApp {
    nymx_config: config::Config,
    sorted_nymx_aliases: Vec<String>,
    selected_nymx_alias: String,
    action: Action,
    file_path: String,
    mode_parts: bool,
    receive_path: String,
    log: String,
    is_running: bool,
    thread_handle: Option<thread::JoinHandle<()>>,
    tx: mpsc::Sender<String>,
    rx: mpsc::Receiver<String>,
    dark_mode: bool,
    show_add_contact: bool,
    show_about: bool,
    show_delete_confirmation: bool,
    new_alias: String,
    new_address: String,
    contact_to_delete: String,
}

impl NymXApp {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        let nymx_config = config::Config::load();
        let mut sorted_nymx_aliases: Vec<String> =
            nymx_config.aliases.keys().cloned().collect();
        sorted_nymx_aliases.sort();
        let selected_nymx_alias =
            sorted_nymx_aliases.first().cloned().unwrap_or_default();
        Self {
            nymx_config,
            sorted_nymx_aliases,
            selected_nymx_alias,
            action: Action::Send,
            file_path: String::new(),
            mode_parts: false,
            receive_path: String::from("./received"),
            log: String::from("NymX ready.\n"),
            is_running: false,
            thread_handle: None,
            tx,
            rx,
            dark_mode: true,
            show_add_contact: false,
            show_about: false,
            show_delete_confirmation: false,
            new_alias: String::new(),
            new_address: String::new(),
            contact_to_delete: String::new(),
        }
    }

    fn extract_prefix_and_folder(&self, file_path: &str) -> (String, String) {
        let path = PathBuf::from(file_path);
        let folder = path.parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| ".".to_string());
        let file_name = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        let stem = if let Some((stem, _ext)) = file_name.rsplit_once('.') {
            stem
        } else {
            file_name
        };
        let prefix = stem.trim_end_matches(|c: char| c.is_ascii_digit());
        (folder, prefix.to_string())
    }

    fn start_action(&mut self) {
        if self.is_running {
            return;
        }
        if let Some(handle) = &self.thread_handle {
            if !handle.is_finished() {
                self.log.push_str("[WARN] Previous thread still active. Please wait.\n");
                return;
            }
        }
        self.thread_handle = None;

        let address = self.nymx_config
            .resolve(&self.selected_nymx_alias)
            .unwrap_or(self.selected_nymx_alias.clone());

        // Arbeitsverzeichnis und Argumente je nach Aktion
        let (working_dir, send_arg) = match self.action {
            Action::Send => {
                if self.mode_parts {
                    let (folder, prefix) = self.extract_prefix_and_folder(&self.file_path);
                    (PathBuf::from(folder), PathBuf::from(prefix))
                } else {
                    (
                        std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
                        PathBuf::from(&self.file_path),
                    )
                }
            }
            Action::Receive => {
                let path = PathBuf::from(&self.receive_path);
                // Ordner ggf. anlegen
                if !path.exists() {
                    if let Err(e) = std::fs::create_dir_all(&path) {
                        self.log.push_str(&format!("[ERROR] Cannot create receive folder: {}\n", e));
                        return;
                    }
                }
                (path, PathBuf::new()) // send_arg wird bei Receive nicht benötigt
            }
        };

        if self.action == Action::Send && self.mode_parts && !working_dir.exists() {
            self.log.push_str(&format!("[ERROR] Folder '{}' does not exist.\n", working_dir.display()));
            return;
        }

        self.is_running = true;
        self.log.push_str("[INFO] Starting task…\n");
        send::set_log_sender(self.tx.clone());
        receive::set_log_sender(self.tx.clone());
        let tx = self.tx.clone();
        let action = self.action;
        let address = address;
        let send_arg = send_arg;
        let working_dir = working_dir;
        let mode_parts = self.mode_parts;

        let handle = thread::spawn(move || {
            let _ = std::env::set_current_dir(&working_dir);
            let rt = tokio::runtime::Runtime::new().unwrap();
            let result = rt.block_on(async {
                match action {
                    Action::Send => {
                        if mode_parts {
                            send::send_parts_mode(address, send_arg).await
                        } else {
                            send::send_mode(address, send_arg).await
                        }
                    }
                    Action::Receive => {
                        receive::receive_mode(None).await
                    }
                }
            });
            if let Err(e) = result {
                let _ = tx.send(format!("[ERROR] {}", e));
            }
            let _ = tx.send("=== Operation finished ===".to_string());
            let _ = tx.send("__DONE__".to_string());
        });
        self.thread_handle = Some(handle);
    }
}

fn load_icon_data() -> egui::IconData {
    let app_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));
    let icon_paths = vec![
        app_dir.join("assets/icon.ico"),
        app_dir.join("icon.ico"),
        PathBuf::from("assets/icon.ico"),
    ];
    for icon_path in icon_paths {
        if let Ok(data) = std::fs::read(&icon_path) {
            if let Ok(img) = image::load_from_memory(&data) {
                let rgba = img.to_rgba8();
                let (width, height) = rgba.dimensions();
                return egui::IconData {
                    rgba: rgba.into_raw(),
                    width,
                    height,
                };
            }
        }
    }
    let data = include_bytes!("../assets/icon.png");
    let img = image::load_from_memory(data).expect("Failed to load icon");
    let rgba = img.to_rgba8();
    let (width, height) = rgba.dimensions();
    egui::IconData {
        rgba: rgba.into_raw(),
        width,
        height,
    }
}

impl eframe::App for NymXApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if let Some(handle) = &self.thread_handle {
            if handle.is_finished() {
                self.thread_handle = None;
                if self.is_running {
                    self.is_running = false;
                }
            }
        }

        while let Ok(msg) = self.rx.try_recv() {
            if msg == "__DONE__" {
                self.is_running = false;
                continue;
            }
            self.log.push_str(&msg);
            self.log.push('\n');
            ctx.request_repaint();
        }

        if self.dark_mode {
            ctx.set_visuals(egui::Visuals::dark());
        } else {
            ctx.set_visuals(egui::Visuals::light());
        }

        let screen_rect = ctx.screen_rect();
        let center = screen_rect.center();

        if self.show_add_contact {
            egui::Window::new("Add Contact")
                .collapsible(false)
                .resizable(false)
                .fixed_size([420.0, 280.0])
                .fixed_pos([center.x - 210.0, center.y - 140.0])
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(10.0);
                        ui.label(egui::RichText::new("Add a new contact").size(16.0).strong());
                        ui.add_space(15.0);
                        ui.horizontal(|ui| {
                            ui.label("Alias:");
                            ui.add(egui::TextEdit::singleline(&mut self.new_alias).desired_width(280.0));
                        });
                        ui.add_space(8.0);
                        ui.horizontal(|ui| {
                            ui.label("Nym Address:");
                            ui.add(egui::TextEdit::singleline(&mut self.new_address).desired_width(280.0).hint_text("Enter Nym address…"));
                        });
                        ui.add_space(15.0);
                        ui.horizontal_centered(|ui| {
                            let add_button = egui::Button::new(
                                egui::RichText::new("Add Contact").color(egui::Color32::WHITE)
                            ).fill(egui::Color32::from_rgb(122, 110, 243));
                            if ui.add(add_button).clicked() {
                                if !self.new_alias.trim().is_empty() && !self.new_address.trim().is_empty() {
                                    self.nymx_config.aliases.insert(self.new_alias.clone(), self.new_address.clone());
                                    if let Err(e) = self.nymx_config.save() {
                                        self.log.push_str(&format!("[ERROR] Failed to save config: {}\n", e));
                                    } else {
                                        self.log.push_str("[INFO] Config saved successfully.\n");
                                    }
                                    self.sorted_nymx_aliases = self.nymx_config.aliases.keys().cloned().collect();
                                    self.sorted_nymx_aliases.sort();
                                    self.selected_nymx_alias = self.new_alias.clone();
                                    self.log.push_str(&format!("[INFO] Added contact: {} -> {}\n", self.new_alias, self.new_address));
                                    self.new_alias.clear();
                                    self.new_address.clear();
                                    self.show_add_contact = false;
                                }
                            }
                            ui.add_space(20.0);
                            if ui.button("Cancel").clicked() {
                                self.new_alias.clear();
                                self.new_address.clear();
                                self.show_add_contact = false;
                            }
                        });
                    });
                });
        }

        if self.show_delete_confirmation && !self.contact_to_delete.is_empty() {
            egui::Window::new("Delete Contact")
                .collapsible(false)
                .resizable(false)
                .fixed_size([350.0, 160.0])
                .fixed_pos([center.x - 175.0, center.y - 80.0])
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(15.0);
                        ui.label(egui::RichText::new(format!("Delete '{}'?", self.contact_to_delete)).size(16.0));
                        ui.add_space(8.0);
                        ui.label(egui::RichText::new("This action cannot be undone.").color(egui::Color32::from_rgb(200, 200, 200)));
                        ui.add_space(15.0);
                        ui.horizontal_centered(|ui| {
                            let delete_button = egui::Button::new(
                                egui::RichText::new("Delete Contact").color(egui::Color32::WHITE)
                            ).fill(egui::Color32::from_rgb(122, 110, 243));
                            if ui.add(delete_button).clicked() {
                                if self.nymx_config.aliases.remove(&self.contact_to_delete).is_some() {
                                    if let Err(e) = self.nymx_config.save() {
                                        self.log.push_str(&format!("[ERROR] Failed to save config: {}\n", e));
                                    } else {
                                        self.log.push_str("[INFO] Config saved successfully.\n");
                                    }
                                    self.sorted_nymx_aliases = self.nymx_config.aliases.keys().cloned().collect();
                                    self.sorted_nymx_aliases.sort();
                                    if self.selected_nymx_alias == self.contact_to_delete {
                                        self.selected_nymx_alias = self.sorted_nymx_aliases.first().cloned().unwrap_or_default();
                                    }
                                    self.log.push_str(&format!("[INFO] Deleted contact: {}\n", self.contact_to_delete));
                                }
                                self.contact_to_delete.clear();
                                self.show_delete_confirmation = false;
                            }
                            ui.add_space(20.0);
                            if ui.button("Cancel").clicked() {
                                self.contact_to_delete.clear();
                                self.show_delete_confirmation = false;
                            }
                        });
                    });
                });
        }

        if self.show_about {
            egui::Window::new("About NymX")
                .collapsible(false)
                .resizable(false)
                .fixed_size([450.0, 320.0])
                .fixed_pos([center.x - 225.0, center.y - 160.0])
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(15.0);
                        ui.label(egui::RichText::new("NymX").size(28.0).strong());
                        ui.add_space(5.0);
                        ui.label(egui::RichText::new("NymX - Anonymous data exchange leveraging SURBs").size(14.0));
                        ui.label(egui::RichText::new("via the Nym Mixnet.").size(14.0));
                        ui.add_space(8.0);
                        ui.label(egui::RichText::new("v0.1.3 (c) 2026 Ch1ffr3punk").size(13.0));
                        ui.add_space(8.0);
                        ui.label(egui::RichText::new("Released under the Apache 2.0 License").size(13.0));
                        ui.add_space(5.0);
                        ui.label(" ");
                        let url = "https://github.com/Ch1ffr3punk/NymX";
                        if ui.hyperlink(url).clicked() {
                            let _ = open::that(url);
                        }
                        ui.add_space(10.0);
                        let close_button = egui::Button::new(
                            egui::RichText::new("Close").color(egui::Color32::WHITE)
                        ).fill(egui::Color32::from_rgb(122, 110, 243));
                        if ui.add(close_button).clicked() {
                            self.show_about = false;
                        }
                    });
                });
        }

        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(5.0);
                ui.horizontal(|ui| {
                    ui.label("To:");
                    egui::ComboBox::from_id_source("nymx_alias_combo")
                        .selected_text(
                            if self.selected_nymx_alias.is_empty() {
                                "Select alias…"
                            } else {
                                &self.selected_nymx_alias
                            }
                        )
                        .show_ui(ui, |ui| {
                            for alias in &self.sorted_nymx_aliases {
                                ui.selectable_value(&mut self.selected_nymx_alias, alias.clone(), alias);
                            }
                        });
                    ui.add_space(8.0);
                    let add_button = egui::Button::new(
                        egui::RichText::new("Add Contact").color(egui::Color32::WHITE)
                    ).fill(egui::Color32::from_rgb(122, 110, 243));
                    if ui.add(add_button).clicked() {
                        self.show_add_contact = true;
                    }
                    ui.add_space(5.0);
                    let delete_enabled = !self.selected_nymx_alias.is_empty();
                    let delete_button = egui::Button::new(
                        egui::RichText::new("Delete Contact").color(egui::Color32::WHITE)
                    ).fill(egui::Color32::from_rgb(122, 110, 243));
                    if ui.add_enabled(delete_enabled, delete_button).clicked() {
                        self.contact_to_delete = self.selected_nymx_alias.clone();
                        self.show_delete_confirmation = true;
                    }
                    ui.add_space(15.0);
                    let about_button = egui::Button::new(
                        egui::RichText::new("About").color(egui::Color32::WHITE)
                    ).fill(egui::Color32::from_rgb(122, 110, 243));
                    if ui.add(about_button).clicked() {
                        self.show_about = true;
                    }
                    ui.add_space(5.0);
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let theme_text = if self.dark_mode { "Light Theme" } else { "Dark Theme" };
                        let theme_button = egui::Button::new(
                            egui::RichText::new(theme_text).color(egui::Color32::WHITE)
                        ).fill(egui::Color32::from_rgb(122, 110, 243));
                        if ui.add(theme_button).clicked() {
                            self.dark_mode = !self.dark_mode;
                        }
                    });
                });
                ui.add_space(5.0);
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.vertical(|ui| {
                ui.horizontal(|ui| {
                    let send_style = if self.action == Action::Send {
                        egui::Button::new(egui::RichText::new("Send").color(egui::Color32::WHITE))
                            .fill(egui::Color32::from_rgb(122, 110, 243))
                    } else {
                        egui::Button::new("Send")
                    };
                    if ui.add(send_style).clicked() {
                        self.action = Action::Send;
                    }
                    let recv_style = if self.action == Action::Receive {
                        egui::Button::new(egui::RichText::new("Receive").color(egui::Color32::WHITE))
                            .fill(egui::Color32::from_rgb(122, 110, 243))
                    } else {
                        egui::Button::new("Receive")
                    };
                    if ui.add(recv_style).clicked() {
                        self.action = Action::Receive;
                    }
                });
                ui.add_space(10.0);
                match self.action {
                    Action::Send => {
                        ui.checkbox(&mut self.mode_parts, "Parts mode (send multiple parts)");
                        if self.mode_parts {
                            ui.horizontal(|ui| {
                                ui.label("Select part file:");
                                if ui.button("Browse…").clicked() {
                                    if let Some(path) = rfd::FileDialog::new().pick_file() {
                                        self.file_path = path.to_string_lossy().to_string();
                                    }
                                }
                            });
                            if !self.file_path.is_empty() {
                                let (folder, prefix) = self.extract_prefix_and_folder(&self.file_path);
                                ui.horizontal(|ui| {
                                    ui.label("Folder:");
                                    ui.label(&folder);
                                });
                                ui.horizontal(|ui| {
                                    ui.label("Prefix:");
                                    ui.label(&prefix);
                                });
                            }
                        } else {
                            ui.horizontal(|ui| {
                                ui.label("File:");
                                ui.text_edit_singleline(&mut self.file_path);
                                if ui.button("Browse…").clicked() {
                                    if let Some(path) = rfd::FileDialog::new().pick_file() {
                                        self.file_path = path.to_string_lossy().to_string();
                                    }
                                }
                            });
                        }
                    }
                    Action::Receive => {
                        ui.horizontal(|ui| {
                            ui.label("Save folder:");
                            ui.text_edit_singleline(&mut self.receive_path);
                            if ui.button("Browse…").clicked() {
                                if let Some(folder) = rfd::FileDialog::new().pick_folder() {
                                    self.receive_path = folder.to_string_lossy().to_string();
                                }
                            }
                        });
                    }
                }
                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    let thread_active = self.thread_handle.as_ref().map_or(false, |h| !h.is_finished());
                    let start_enabled = !self.is_running && !thread_active
                        && match self.action {
                            Action::Send => {
                                if self.mode_parts {
                                    !self.file_path.trim().is_empty()
                                        && !self.selected_nymx_alias.is_empty()
                                        && {
                                            let (folder, _) = self.extract_prefix_and_folder(&self.file_path);
                                            let folder_path = PathBuf::from(&folder);
                                            folder_path.exists()
                                        }
                                } else {
                                    !self.file_path.trim().is_empty() && !self.selected_nymx_alias.is_empty()
                                }
                            }
                            Action::Receive => !self.receive_path.trim().is_empty(),
                        };
                    let start_button = egui::Button::new(
                        egui::RichText::new(if self.is_running { "Running…" } else { "Start" })
                            .color(egui::Color32::WHITE)
                    ).fill(egui::Color32::from_rgb(122, 110, 243));
                    if ui.add_enabled(start_enabled, start_button).clicked() {
                        self.start_action();
                    }
                    if self.is_running {
                        ui.label(egui::RichText::new("Processing…").color(egui::Color32::LIGHT_BLUE));
                        ui.add_space(10.0);
                        let end_button = egui::Button::new(
                            egui::RichText::new("Quit Session").color(egui::Color32::WHITE)
                        ).fill(egui::Color32::from_rgb(200, 50, 50));
                        if ui.add(end_button).clicked() {
                            self.is_running = false;
                            self.log.push_str("[INFO] Session quit by user.\n");
                        }
                    }
                });
                ui.add_space(12.0);
                ui.group(|ui| {
                    ui.label("Log");
                    let log_height = (ui.available_height() - 20.0).max(120.0);
                    egui::ScrollArea::vertical()
                        .max_height(log_height)
                        .auto_shrink([false; 2])
                        .stick_to_bottom(true)
                        .show(ui, |ui| {
                            ui.add(egui::Label::new(self.log.clone()).selectable(true));
                        });
                });
                ui.add_space(8.0);
            });
        });
    }
}

fn main() -> Result<(), eframe::Error> {
    let icon_data = load_icon_data();
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([720.0, 720.0])
            .with_title("NymX")
            .with_resizable(true)
            .with_icon(std::sync::Arc::new(icon_data)),
        ..Default::default()
    };
    eframe::run_native(
        "NymX",
        options,
        Box::new(|_cc| Ok(Box::new(NymXApp::new()))),
    )
}
